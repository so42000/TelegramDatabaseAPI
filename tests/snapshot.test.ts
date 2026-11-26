import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { hashToken, importMasterKey, newBucketToken, newKeyId } from "../src/crypto";
import { openDb, type IndexDb } from "../src/db";
import { BotPool } from "../src/pool";
import { MANIFEST_CAPTION, Snapshot } from "../src/snapshot";
import { FAKE_BOT_ID, FAKE_CHANNEL_ID, startFakeTelegram, type FakeTelegram } from "./fake-telegram";

let fake: FakeTelegram;
let dirs: string[] = [];

beforeEach(() => {
  fake = startFakeTelegram();
});

afterEach(() => {
  fake.stop();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function tempDb(name: string): IndexDb {
  const dir = mkdtempSync(join(tmpdir(), "tgstore-snap-"));
  dirs.push(dir);
  return openDb(join(dir, `${name}.sqlite`));
}

// Object bytes live on Telegram and are never read by a snapshot, so the parts only need to be well-formed rows.
function addObject(db: IndexDb, bucket: string, key: string, n: number): void {
  const objectId = crypto.randomUUID();
  db.commitParts([
    { objectId, partNo: 0, messageId: 1000 + n, fileId: `file-${n}`, botId: FAKE_BOT_ID, blobOffset: 0, blobLength: 40, size: 11 },
  ]);
  const result = db.commitObject(
    {
      bucket,
      key,
      objectId,
      contentType: "text/plain",
      size: 11,
      sha256: "ab".repeat(32),
      meta: JSON.stringify({ n }),
      updatedAt: n,
      partCount: 1,
    },
    null,
  );
  expect(result).toBe("created");
}

describe("chunked snapshot", () => {
  test("a snapshot split into several parts restores identically and the previous parts are deleted", async () => {
    const pool = await BotPool.connect({
      apiBase: fake.apiBase,
      tokens: [`${FAKE_BOT_ID}:test-token`],
      minIntervalMs: 0,
    });
    const key = await importMasterKey(crypto.getRandomValues(new Uint8Array(32)));
    const tuning = { pageRows: 2, partMaxBytes: 1 };
    const buckets = ["b1", "b2", "b3"];

    const source = tempDb("source");
    for (const name of buckets) {
      source.createBucket(name, 1);
      source.insertKey({
        id: newKeyId(),
        bucket: name,
        tokenHash: hashToken(newBucketToken()),
        scope: "write",
        label: `label-${name}`,
        createdAt: 1,
        expiresAt: null,
      });
    }
    for (let i = 0; i < 12; i++) addObject(source, buckets[i % 3]!, `k${i}`, i);

    const snapshot = new Snapshot({ db: source, pool, channelId: FAKE_CHANNEL_ID, key, intervalMs: 3_600_000, tuning });
    snapshot.markDirty();
    await snapshot.flush();

    const pinnedId = fake.state.pinned;
    if (pinnedId === null) throw new Error("snapshot was not pinned");
    expect(fake.state.messages.get(pinnedId)?.caption).toBe(MANIFEST_CAPTION);
    const firstParts = [...fake.state.messages.keys()].filter((id) => id !== pinnedId);
    expect(firstParts.length).toBeGreaterThanOrEqual(3);

    const restored = tempDb("restored");
    const restorer = new Snapshot({ db: restored, pool, channelId: FAKE_CHANNEL_ID, key, intervalMs: 3_600_000, tuning });
    expect(await restorer.restoreIfEmpty()).toBe("restored");

    expect(restored.listBuckets()).toEqual(source.listBuckets());
    for (const bucket of buckets) {
      expect(restored.listKeys(bucket)).toEqual(source.listKeys(bucket));
      const objects = source.listObjects(bucket, "", 1000, null);
      expect(restored.listObjects(bucket, "", 1000, null)).toEqual(objects);
      for (const object of objects) {
        expect(restored.getParts(object.objectId)).toEqual(source.getParts(object.objectId));
      }
    }

    snapshot.markDirty();
    await snapshot.flush();
    for (const id of firstParts) expect(fake.state.deleted).toContain(id);

    source.close();
    restored.close();
  }, 30_000);
});
