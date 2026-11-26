import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomFillSync } from "node:crypto";
import { createApp, type App } from "../src/app";
import type { Config } from "../src/config";
import { encryptBlob, hashToken, importMasterKey, newBucketToken, sha256Hex } from "../src/crypto";
import { LEGACY_SNAPSHOT_AAD, LEGACY_SNAPSHOT_CAPTION } from "../src/snapshot";
import { FAKE_BOT_ID, FAKE_CHANNEL_ID, startFakeTelegram, type FakeTelegram } from "./fake-telegram";

const TOKEN_RE = /tgs_[A-Za-z0-9_-]{43}/;
const OWNER_ID = 42;

let fake: FakeTelegram;
let dirs: string[] = [];
let running: App[] = [];

beforeEach(() => {
  fake = startFakeTelegram();
});

afterEach(async () => {
  for (const app of running.splice(0)) await app.stop();
  fake.stop();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function botTokens(n: number): string[] {
  return Array.from({ length: n }, (_, i) => `${FAKE_BOT_ID + i}:test-token`);
}

function makeConfig(over: Partial<Config> = {}): Config {
  const dir = mkdtempSync(join(tmpdir(), "tgstore-"));
  dirs.push(dir);
  return {
    botTokens: botTokens(1),
    channelId: FAKE_CHANNEL_ID,
    ownerIds: new Set([OWNER_ID]),
    masterKey: crypto.getRandomValues(new Uint8Array(32)),
    port: 0,
    dbPath: join(dir, "index.sqlite"),
    apiBase: fake.apiBase,
    snapshotIntervalMs: 3_600_000,
    channelMinIntervalMs: 0,
    queueMaxBytes: 67_108_864,
    maxObjectBytes: 5_368_709_120,
    adminToken: null,
    botPolling: true,
    ...over,
  };
}

// Env for a CLI child process. Explicit keys override anything Bun auto-loads from .env in the cwd.
function cliEnv(config: Config, extra: Record<string, string> = {}): Record<string, string> {
  return {
    PATH: process.env.PATH ?? "",
    HOME: process.env.HOME ?? "",
    TELEGRAM_BOT_TOKENS: config.botTokens.join(","),
    STORAGE_CHANNEL_ID: config.channelId,
    OWNER_IDS: [...config.ownerIds].join(","),
    MASTER_KEY: Buffer.from(config.masterKey).toString("base64"),
    DB_PATH: config.dbPath,
    TELEGRAM_API_BASE: config.apiBase,
    CHANNEL_MIN_INTERVAL_MS: "0",
    BOT_POLLING: "false",
    ...extra,
  };
}

async function runCli(script: string, args: string[], env: Record<string, string>): Promise<{ code: number; out: string; err: string }> {
  const proc = Bun.spawn([process.execPath, join(import.meta.dir, "..", "src", "cli", script), ...args], {
    env,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [out, err] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text()]);
  return { code: await proc.exited, out, err };
}

// Starts an app whose stop() is idempotent, so afterEach can stop it even after a test already did.
async function launch(config: Config): Promise<App> {
  const app = await createApp(config);
  let stopped = false;
  const handle: App = {
    port: app.port,
    stop: async () => {
      if (stopped) return;
      stopped = true;
      await app.stop();
    },
  };
  running.push(handle);
  return handle;
}

function ownerMessage(extra: Record<string, unknown>, chatId = OWNER_ID, userId = OWNER_ID): Record<string, unknown> {
  return {
    chat: { id: chatId, type: "private" },
    from: { id: userId, is_bot: false, first_name: "owner" },
    ...extra,
  };
}

// Sends /newbucket through the bot and returns the password from the reply.
async function createBucketViaBot(name: string): Promise<string> {
  const before = fake.state.sent.length;
  await fake.state.sendUpdate(ownerMessage({ text: `/newbucket ${name}` }));
  const token = fake.state.sent
    .slice(before)
    .map((s) => s.text)
    .join("\n")
    .match(TOKEN_RE)?.[0];
  if (token === undefined) throw new Error("bot reply has no bucket password");
  return token;
}

function objectUrl(app: App, bucket: string, key: string): string {
  return `http://localhost:${app.port}/v1/buckets/${bucket}/objects/${encodeURI(key)}`;
}

function authHeaders(token: string, contentType = "application/octet-stream"): Record<string, string> {
  return { Authorization: `Bearer ${token}`, "Content-Type": contentType };
}

function putObject(app: App, bucket: string, key: string, token: string, body: BodyInit, contentType?: string) {
  return fetch(objectUrl(app, bucket, key), {
    method: "PUT",
    headers: authHeaders(token, contentType),
    body,
  });
}

function sendDocumentCount(): number {
  return fake.state.calls.filter((c) => c === "sendDocument").length;
}

function sendDocumentBots(from: number): number[] {
  return fake.state.botCalls.slice(from).filter((c) => c.method === "sendDocument").map((c) => c.botId);
}

describe("tg storage app", () => {
  test("round trip: PUT then GET returns the same bytes; Telegram only sees ciphertext", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");
    const body = JSON.stringify({ name: "Alice" });
    const put = await putObject(app, "myapp", "users/1", token, body, "application/json");
    expect(put.status).toBe(201);
    const meta = (await put.json()) as { key: string; etag: string };
    expect(meta.key).toBe("users/1");
    expect(meta.etag).toBe(new Bun.CryptoHasher("sha256").update(body).digest("hex"));

    const get = await fetch(objectUrl(app, "myapp", "users/1"), {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(get.status).toBe(200);
    expect(get.headers.get("content-type")).toBe("application/json");
    expect(get.headers.get("etag")).toBe(`"${meta.etag}"`);
    expect(await get.text()).toBe(body);

    const blobs = [...fake.state.files.values()];
    expect(blobs.length).toBeGreaterThan(0);
    for (const blob of blobs) expect(Buffer.from(blob).includes("Alice")).toBe(false);
  });

  test("wrong password and another bucket's password are both rejected with 401", async () => {
    const app = await launch(makeConfig());
    const myappToken = await createBucketViaBot("myapp");
    const otherToken = await createBucketViaBot("other");
    expect(myappToken).not.toBe(otherToken);

    const wrong = await putObject(app, "myapp", "k", "tgs_" + "A".repeat(43), "x");
    expect(wrong.status).toBe(401);

    const cross = await putObject(app, "myapp", "k", otherToken, "x");
    expect(cross.status).toBe(401);
  });

  test("a non-owner /newbucket is ignored: no reply and no bucket", async () => {
    const app = await launch(makeConfig());
    await fake.state.sendUpdate(ownerMessage({ text: "/newbucket hacker" }, 7, 7));
    expect(fake.state.sent).toHaveLength(0);

    const res = await fetch(objectUrl(app, "hacker", "k"), { method: "PUT", body: "x" });
    expect(res.status).toBe(401);
  });

  test("overwrite returns 201 then 200 and the replaced message is deleted on flush", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");

    expect((await putObject(app, "myapp", "k", token, "v1")).status).toBe(201);
    const v1Id = [...fake.state.messages.keys()].at(-1);
    expect(v1Id).toBeDefined();

    expect((await putObject(app, "myapp", "k", token, "v2")).status).toBe(200);
    const get = await fetch(objectUrl(app, "myapp", "k"), { headers: { Authorization: `Bearer ${token}` } });
    expect(await get.text()).toBe("v2");

    await app.stop();
    expect(fake.state.deleted).toContain(v1Id as number);
  });

  test("restore: a new empty DB with the same MASTER_KEY recovers buckets and objects from the pinned snapshot", async () => {
    const masterKey = crypto.getRandomValues(new Uint8Array(32));
    const first = await launch(makeConfig({ masterKey }));
    const token = await createBucketViaBot("myapp");
    expect((await putObject(first, "myapp", "docs/a.txt", token, "persisted")).status).toBe(201);
    await first.stop();

    const second = await launch(makeConfig({ masterKey }));
    const get = await fetch(objectUrl(second, "myapp", "docs/a.txt"), {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(get.status).toBe(200);
    expect(await get.text()).toBe("persisted");
  });

  test("wrong MASTER_KEY refuses to start with a snapshot decrypt error", async () => {
    const first = await launch(makeConfig());
    await createBucketViaBot("myapp");
    await first.stop();

    await expect(createApp(makeConfig())).rejects.toThrow(/snapshot decrypt failed/);
  });

  test("a body over maxObjectBytes gets 413 before upload", async () => {
    const app = await launch(makeConfig({ maxObjectBytes: 1024 }));
    const token = await createBucketViaBot("myapp");
    const before = sendDocumentCount();

    const res = await putObject(app, "myapp", "big.bin", token, new Uint8Array(1025));
    expect(res.status).toBe(413);
    expect(await res.json()).toEqual({ error: "too_large", maxBytes: 1024 });
    expect(sendDocumentCount()).toBe(before);
  });

  test("a 40 MiB object is split into 3 parts and reads back whole", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");
    const before = sendDocumentCount();
    const source = randomFillSync(Buffer.alloc(40 * 1024 * 1024));
    const sha = new Bun.CryptoHasher("sha256").update(source).digest("hex");

    const put = await putObject(app, "myapp", "big.bin", token, source);
    expect(put.status).toBe(201);
    expect(((await put.json()) as { etag: string }).etag).toBe(sha);
    expect(sendDocumentCount() - before).toBeGreaterThanOrEqual(3);

    const get = await fetch(objectUrl(app, "myapp", "big.bin"), {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(get.status).toBe(200);
    expect(Buffer.from(await get.arrayBuffer()).equals(source)).toBe(true);
    expect(get.headers.get("etag")).toBe(`"${sha}"`);

    const ranged = await fetch(objectUrl(app, "myapp", "big.bin"), {
      headers: { Authorization: `Bearer ${token}`, Range: "bytes=19922940-19922950" },
    });
    expect(ranged.status).toBe(206);
    expect(ranged.headers.get("content-range")).toBe("bytes 19922940-19922950/41943040");
    expect(Buffer.from(await ranged.arrayBuffer()).equals(source.subarray(19922940, 19922951))).toBe(true);
  }, 60_000);

  test("HEAD returns metadata headers and an empty body", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");
    await putObject(app, "myapp", "d.txt", token, "0123456789");

    const res = await fetch(objectUrl(app, "myapp", "d.txt"), {
      method: "HEAD",
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(res.status).toBe(200);
    expect(res.headers.get("content-length")).toBe("10");
    expect(res.headers.get("accept-ranges")).toBe("bytes");
    expect(res.headers.get("x-content-type-options")).toBe("nosniff");
    expect(await res.text()).toBe("");
  });

  test("Range returns 206 slices and 416 when unsatisfiable", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");
    await putObject(app, "myapp", "d.txt", token, "0123456789");
    const get = (range: string) =>
      fetch(objectUrl(app, "myapp", "d.txt"), { headers: { Authorization: `Bearer ${token}`, Range: range } });

    const middle = await get("bytes=2-5");
    expect(middle.status).toBe(206);
    expect(middle.headers.get("content-range")).toBe("bytes 2-5/10");
    expect(await middle.text()).toBe("2345");

    const suffix = await get("bytes=-3");
    expect(suffix.status).toBe(206);
    expect(await suffix.text()).toBe("789");

    const unsatisfiable = await get("bytes=10-");
    expect(unsatisfiable.status).toBe(416);
    expect(unsatisfiable.headers.get("content-range")).toBe("bytes */10");
  });

  test("conditional GET, PUT and DELETE follow the etag", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");
    await putObject(app, "myapp", "d.txt", token, "0123456789");
    const etag = new Bun.CryptoHasher("sha256").update("0123456789").digest("hex");
    const url = objectUrl(app, "myapp", "d.txt");
    const auth = { Authorization: `Bearer ${token}` };

    const notModified = await fetch(url, { headers: { ...auth, "If-None-Match": `"${etag}"` } });
    expect(notModified.status).toBe(304);

    const createOnly = await fetch(url, { method: "PUT", headers: { ...auth, "If-None-Match": "*" }, body: "new" });
    expect(createOnly.status).toBe(412);
    expect(await (await fetch(url, { headers: auth })).text()).toBe("0123456789");

    const wrongMatch = await fetch(url, { method: "PUT", headers: { ...auth, "If-Match": '"wrong"' }, body: "new" });
    expect(wrongMatch.status).toBe(412);

    const rightMatch = await fetch(url, { method: "PUT", headers: { ...auth, "If-Match": `"${etag}"` }, body: "abc" });
    expect(rightMatch.status).toBe(200);

    const wrongDelete = await fetch(url, { method: "DELETE", headers: { ...auth, "If-Match": '"wrong"' } });
    expect(wrongDelete.status).toBe(412);
  });

  test("x-meta headers are stored, returned on HEAD and listed; invalid names are rejected", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");
    const auth = { Authorization: `Bearer ${token}` };
    const url = objectUrl(app, "myapp", "d.txt");

    const put = await fetch(url, { method: "PUT", headers: { ...auth, "x-meta-owner": "alice" }, body: "0123456789" });
    expect(put.status).toBe(201);
    expect(((await put.json()) as { metadata: unknown }).metadata).toEqual({ owner: "alice" });

    const head = await fetch(url, { method: "HEAD", headers: auth });
    expect(head.headers.get("x-meta-owner")).toBe("alice");

    const list = await fetch(`http://localhost:${app.port}/v1/buckets/myapp/objects`, { headers: auth });
    const listed = (await list.json()) as { objects: { key: string; metadata: Record<string, string> }[] };
    expect(listed.objects.find((o) => o.key === "d.txt")?.metadata).toEqual({ owner: "alice" });

    const bad = await fetch(url, { method: "PUT", headers: { ...auth, "x-meta-bad_name": "x" }, body: "x" });
    expect(bad.status).toBe(400);
    expect(await bad.json()).toEqual({ error: "invalid_metadata", detail: "invalid metadata name: bad_name" });
  });

  test("admin API is off without ADMIN_TOKEN", async () => {
    const app = await launch(makeConfig());
    const res = await fetch(`http://localhost:${app.port}/v1/admin/buckets`, {
      headers: { Authorization: `Bearer ${"a".repeat(40)}` },
    });
    expect(res.status).toBe(404);
  });

  test("admin creates a bucket; a read key can GET but not PUT; a revoked key is rejected", async () => {
    const adminToken = "a".repeat(40);
    const app = await launch(makeConfig({ adminToken }));
    const admin = { Authorization: `Bearer ${adminToken}`, "Content-Type": "application/json" };
    const base = `http://localhost:${app.port}/v1/admin`;

    const created = await fetch(`${base}/buckets`, {
      method: "POST",
      headers: admin,
      body: JSON.stringify({ name: "photos" }),
    });
    expect(created.status).toBe(201);
    const writer = { Authorization: `Bearer ${((await created.json()) as { key: { token: string } }).key.token}` };

    const readKey = await fetch(`${base}/buckets/photos/keys`, {
      method: "POST",
      headers: admin,
      body: JSON.stringify({ scope: "read", label: "viewer" }),
    });
    expect(readKey.status).toBe(201);
    const issued = ((await readKey.json()) as { key: { id: string; token: string } }).key;
    const reader = { Authorization: `Bearer ${issued.token}` };

    const url = objectUrl(app, "photos", "a.txt");
    expect((await fetch(url, { method: "PUT", headers: writer, body: "hi" })).status).toBe(201);
    expect((await fetch(url, { headers: reader })).status).toBe(200);
    expect((await fetch(url, { method: "PUT", headers: reader, body: "no" })).status).toBe(403);

    const revoked = await fetch(`${base}/buckets/photos/keys/${issued.id}`, { method: "DELETE", headers: admin });
    expect(revoked.status).toBe(204);
    expect((await fetch(url, { headers: reader })).status).toBe(401);

    const wrongAdmin = await fetch(`${base}/buckets`, { headers: { Authorization: `Bearer ${"b".repeat(40)}` } });
    expect(wrongAdmin.status).toBe(401);
  });

  test("rotate keeps old keys for the grace period and graceSeconds 0 cuts them off", async () => {
    const adminToken = "a".repeat(40);
    const app = await launch(makeConfig({ adminToken }));
    const admin = { Authorization: `Bearer ${adminToken}`, "Content-Type": "application/json" };
    const base = `http://localhost:${app.port}/v1/admin`;
    const url = objectUrl(app, "docs", "n.txt");

    const created = await fetch(`${base}/buckets`, {
      method: "POST",
      headers: admin,
      body: JSON.stringify({ name: "docs" }),
    });
    const first = ((await created.json()) as { key: { token: string } }).key.token;
    const firstAuth = { Authorization: `Bearer ${first}` };
    expect((await fetch(url, { method: "PUT", headers: firstAuth, body: "v1" })).status).toBe(201);

    const rotate = async (body?: object): Promise<string> => {
      const res = await fetch(`${base}/buckets/docs/rotate`, {
        method: "POST",
        headers: admin,
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      expect(res.status).toBe(201);
      return ((await res.json()) as { key: { token: string } }).key.token;
    };
    const get = (token: string) => fetch(url, { headers: { Authorization: `Bearer ${token}` } });

    const second = await rotate({ graceSeconds: 3600 });
    expect((await get(first)).status).toBe(200);
    expect((await get(second)).status).toBe(200);

    const third = await rotate({ graceSeconds: 0 });
    expect((await get(first)).status).toBe(401);
    expect((await get(second)).status).toBe(401);
    expect((await get(third)).status).toBe(200);
  });

  test("admin rejects a bad bucket name or scope with 400", async () => {
    const adminToken = "a".repeat(40);
    const app = await launch(makeConfig({ adminToken }));
    const admin = { Authorization: `Bearer ${adminToken}`, "Content-Type": "application/json" };
    const base = `http://localhost:${app.port}/v1/admin`;

    const badName = await fetch(`${base}/buckets`, {
      method: "POST",
      headers: admin,
      body: JSON.stringify({ name: "Bad Name" }),
    });
    expect(badName.status).toBe(400);
    expect(((await badName.json()) as { error: string }).error).toBe("invalid_name");

    await fetch(`${base}/buckets`, { method: "POST", headers: admin, body: JSON.stringify({ name: "ok" }) });
    const badScope = await fetch(`${base}/buckets/ok/keys`, {
      method: "POST",
      headers: admin,
      body: JSON.stringify({ scope: "admin" }),
    });
    expect(badScope.status).toBe(400);
    expect(((await badScope.json()) as { error: string }).error).toBe("invalid_request");
  });

  test("a Telegram 429 on sendDocument is retried and the PUT still succeeds", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");
    const before = sendDocumentCount();

    fake.state.fail429.add("sendDocument");
    const res = await putObject(app, "myapp", "retry.txt", token, "after rate limit");
    expect(res.status).toBe(201);
    expect(sendDocumentCount() - before).toBe(2);
  }, 10_000);

  test("a PUT delayed 12 s by a Telegram 429 back-off still gets its 201", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");

    fake.state.retryAfterS = 12;
    fake.state.fail429.add("sendDocument");
    const res = await putObject(app, "myapp", "slow.txt", token, "after long back-off");
    expect(res.status).toBe(201);
  }, 30_000);

  test("stop() lets an in-flight PUT finish and keeps it", async () => {
    const config = makeConfig();
    const app = await launch(config);
    const token = await createBucketViaBot("myapp");
    const before = sendDocumentCount();

    fake.state.retryAfterS = 3;
    fake.state.fail429.add("sendDocument");
    const put = putObject(app, "myapp", "inflight.txt", token, "kept across stop");
    while (sendDocumentCount() === before) await Bun.sleep(10);
    await app.stop();
    expect((await put).status).toBe(201);

    const restarted = await launch(config);
    const get = await fetch(objectUrl(restarted, "myapp", "inflight.txt"), {
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(get.status).toBe(200);
    expect(await get.text()).toBe("kept across stop");
  }, 15_000);

  test("listing filters by prefix and pages with the cursor", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");
    for (const key of ["logs/a", "logs/b", "logs/c", "other/x"]) {
      expect((await putObject(app, "myapp", key, token, key)).status).toBe(201);
    }

    const auth = { headers: { Authorization: `Bearer ${token}` } };
    const base = `http://localhost:${app.port}/v1/buckets/myapp/objects`;

    const page1 = (await (await fetch(`${base}?prefix=logs/&limit=2`, auth)).json()) as {
      objects: { key: string }[];
      nextCursor: string | null;
    };
    expect(page1.objects.map((o) => o.key)).toEqual(["logs/a", "logs/b"]);
    expect(page1.nextCursor).not.toBeNull();

    const page2 = (await (
      await fetch(`${base}?prefix=logs/&limit=2&cursor=${encodeURIComponent(page1.nextCursor ?? "")}`, auth)
    ).json()) as { objects: { key: string }[]; nextCursor: string | null };
    expect(page2.objects.map((o) => o.key)).toEqual(["logs/c"]);
    expect(page2.nextCursor).toBeNull();
  });

  test("owner uploads a photo to the bot with caption and gets the bytes back with image/jpeg", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("myapp");

    const jpeg = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 1, 2, 3, 4, 5]);
    fake.state.files.set("photo-1", jpeg);
    const before = fake.state.sent.length;
    await fake.state.sendUpdate(
      ownerMessage({
        caption: "myapp pics/cat",
        photo: [{ file_id: "photo-1", file_unique_id: "u1", width: 90, height: 90, file_size: jpeg.byteLength }],
      }),
    );
    expect(fake.state.sent.slice(before).some((s) => s.text.startsWith("Stored"))).toBe(true);

    const get = await fetch(objectUrl(app, "myapp", "pics/cat"), { headers: { Authorization: `Bearer ${token}` } });
    expect(get.status).toBe(200);
    expect(get.headers.get("content-type")).toBe("image/jpeg");
    expect(new Uint8Array(await get.arrayBuffer())).toEqual(jpeg);
  });

  test("PUTs that share one channel slot are stored in a single sendDocument", async () => {
    const app = await launch(makeConfig({ channelMinIntervalMs: 1000 }));
    const token = await createBucketViaBot("myapp");
    expect((await putObject(app, "myapp", "warm", token, "warm")).status).toBe(201);

    const before = sendDocumentCount();
    const puts = await Promise.all(
      Array.from({ length: 10 }, (_, i) => putObject(app, "myapp", `k${i}`, token, `body-${i}`)),
    );
    expect(puts.map((r) => r.status)).toEqual(Array(10).fill(201));
    expect(sendDocumentCount() - before).toBe(1);

    for (let i = 0; i < 10; i++) {
      const get = await fetch(objectUrl(app, "myapp", `k${i}`), { headers: { Authorization: `Bearer ${token}` } });
      expect(await get.text()).toBe(`body-${i}`);
    }
  }, 10_000);

  test("a shared batch message is deleted only after its last object is replaced", async () => {
    const app = await launch(makeConfig({ channelMinIntervalMs: 1000 }));
    const token = await createBucketViaBot("myapp");
    expect((await putObject(app, "myapp", "warm", token, "warm")).status).toBe(201);

    const [a, b] = await Promise.all([
      putObject(app, "myapp", "a", token, "a"),
      putObject(app, "myapp", "b", token, "b"),
    ]);
    expect([a.status, b.status]).toEqual([201, 201]);
    const batch = [...fake.state.messages.keys()].at(-1);
    if (batch === undefined) throw new Error("no batch message");

    expect((await putObject(app, "myapp", "a", token, "a2")).status).toBe(200);
    await createBucketViaBot("flush1");
    expect(fake.state.deleted).not.toContain(batch);
    const getB = await fetch(objectUrl(app, "myapp", "b"), { headers: { Authorization: `Bearer ${token}` } });
    expect(await getB.text()).toBe("b");

    expect((await putObject(app, "myapp", "b", token, "b2")).status).toBe(200);
    await createBucketViaBot("flush2");
    expect(fake.state.deleted).toContain(batch);
  }, 15_000);

  test("restores a v1 snapshot whose objects each own a whole Telegram document", async () => {
    const masterKey = crypto.getRandomValues(new Uint8Array(32));
    const key = await importMasterKey(masterKey);
    const token = newBucketToken();
    const objectId = crypto.randomUUID();
    const body = new TextEncoder().encode("legacy");
    fake.state.files.set("legacy-obj", await encryptBlob(key, objectId, body));

    const snapshot = {
      v: 1,
      createdAt: Date.now(),
      buckets: [{ name: "old", tokenHash: hashToken(token), createdAt: 1 }],
      objects: [
        {
          bucket: "old",
          key: "x",
          objectId,
          messageId: 501,
          fileId: "legacy-obj",
          contentType: "text/plain",
          size: body.byteLength,
          sha256: sha256Hex(body),
          updatedAt: 1,
        },
      ],
    };
    const payload = Bun.gzipSync(new TextEncoder().encode(JSON.stringify(snapshot)));
    fake.state.files.set("legacy-snap", await encryptBlob(key, LEGACY_SNAPSHOT_AAD, payload));
    fake.state.messages.set(500, {
      message_id: 500,
      date: 0,
      chat: { id: Number(FAKE_CHANNEL_ID), type: "channel" },
      caption: LEGACY_SNAPSHOT_CAPTION,
      document: { file_id: "legacy-snap", file_unique_id: "legacy-snap", file_size: 1 },
    });
    fake.state.pinned = 500;

    const app = await launch(makeConfig({ masterKey }));
    const get = await fetch(objectUrl(app, "old", "x"), { headers: { Authorization: `Bearer ${token}` } });
    expect(get.status).toBe(200);
    expect(get.headers.get("content-type")).toBe("text/plain");
    expect(await get.text()).toBe("legacy");
  });

  test("with 10 bots, back-to-back writes go to idle bots instead of waiting for one bot's slot", async () => {
    const app = await launch(makeConfig({ botTokens: botTokens(10), channelMinIntervalMs: 2000 }));
    const token = await createBucketViaBot("myapp");

    const from = fake.state.botCalls.length;
    const t0 = performance.now();
    const statuses: number[] = [];
    for (let i = 0; i < 3; i++) statuses.push((await putObject(app, "myapp", `k${i}`, token, `v${i}`)).status);
    const elapsed = performance.now() - t0;

    expect(statuses).toEqual([201, 201, 201]);
    expect(sendDocumentBots(from)).toEqual([FAKE_BOT_ID + 1, FAKE_BOT_ID + 2, FAKE_BOT_ID + 3]);
    expect(elapsed).toBeLessThan(1500);
    for (let i = 0; i < 3; i++) {
      const get = await fetch(objectUrl(app, "myapp", `k${i}`), { headers: { Authorization: `Bearer ${token}` } });
      expect(await get.text()).toBe(`v${i}`);
    }
  }, 15_000);

  test("the owner can store a file through bot #3 and fetch it through bot #1", async () => {
    const app = await launch(makeConfig({ botTokens: botTokens(3) }));
    await createBucketViaBot("myapp");
    fake.state.files.set("doc-3", new TextEncoder().encode("from bot 3"));
    fake.state.fileOwners.set("doc-3", FAKE_BOT_ID + 2);

    const from = fake.state.botCalls.length;
    const sentBefore = fake.state.sent.length;
    await fake.state.sendUpdate(
      ownerMessage({
        caption: "myapp notes/a",
        document: { file_id: "doc-3", file_unique_id: "doc-3", file_size: 10, mime_type: "text/plain" },
      }),
      FAKE_BOT_ID + 2,
    );
    expect(fake.state.sent.slice(sentBefore)[0]?.text.startsWith("Stored")).toBe(true);
    expect(sendDocumentBots(from)).toEqual([FAKE_BOT_ID + 1]);

    await fake.state.sendUpdate(ownerMessage({ text: "/get myapp notes/a" }));
    const reply = [...fake.state.messages.values()].filter((m) => m.chat.id === OWNER_ID).at(-1);
    const bytes = reply?.document ? fake.state.files.get(reply.document.file_id) : undefined;
    if (!bytes) throw new Error("no document reply for /get");
    expect(new TextDecoder().decode(bytes)).toBe("from bot 3");
    await app.stop();
  });

  test("objects stored by a bot removed from the config are taken over on first read", async () => {
    const masterKey = crypto.getRandomValues(new Uint8Array(32));
    const tokens = botTokens(3);
    const cfg = makeConfig({ masterKey, botTokens: tokens });
    const app = await launch(cfg);
    const token = await createBucketViaBot("myapp");
    const from = fake.state.botCalls.length;
    expect((await putObject(app, "myapp", "k", token, "kept")).status).toBe(201);
    expect(sendDocumentBots(from)).toEqual([FAKE_BOT_ID + 1]);
    await app.stop();

    const relaunched = await launch(
      makeConfig({ masterKey, dbPath: cfg.dbPath, botTokens: tokens.filter((_, i) => i !== 1) }),
    );
    for (let i = 0; i < 2; i++) {
      const get = await fetch(objectUrl(relaunched, "myapp", "k"), { headers: { Authorization: `Bearer ${token}` } });
      expect(get.status).toBe(200);
      expect(await get.text()).toBe("kept");
    }
    expect(fake.state.botCalls.filter((c) => c.method === "editMessageCaption")).toHaveLength(1);
  });

  test("a bot that loses access mid-run is dropped; its writes and reads move to the remaining bots", async () => {
    const app = await launch(makeConfig({ botTokens: botTokens(3) }));
    const token = await createBucketViaBot("myapp");
    expect((await putObject(app, "myapp", "a", token, "a")).status).toBe(201);

    fake.state.lostBots.add(FAKE_BOT_ID + 2);
    const from = fake.state.botCalls.length;
    expect((await putObject(app, "myapp", "b", token, "b")).status).toBe(201);
    expect(sendDocumentBots(from)).toEqual([FAKE_BOT_ID + 2, FAKE_BOT_ID]);

    fake.state.lostBots.add(FAKE_BOT_ID + 1);
    for (const key of ["a", "b"]) {
      const get = await fetch(objectUrl(app, "myapp", key), { headers: { Authorization: `Bearer ${token}` } });
      expect(get.status).toBe(200);
      expect(await get.text()).toBe(key);
    }
    expect(fake.state.botCalls).toContainEqual({ botId: FAKE_BOT_ID, method: "editMessageCaption" });
  });

  test("metrics and request ids", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("metricsapp");
    const res = await fetch(objectUrl(app, "metricsapp", "a.txt"), {
      method: "PUT",
      headers: { ...authHeaders(token, "text/plain"), "X-Request-Id": "test-123" },
      body: "hello",
    });
    expect(res.status).toBe(201);
    expect(res.headers.get("x-request-id")).toBe("test-123");
    const metrics = await (await fetch(`http://localhost:${app.port}/metrics`)).text();
    expect(metrics).toContain('tgstore_http_requests_total{route="object.put",method="PUT",status="201"}');
    expect(metrics).toContain("tgstore_bots_enabled 1");
  });

  test("readyz turns 503 when Telegram is unreachable", async () => {
    const app = await launch(makeConfig());
    const token = await createBucketViaBot("readyapp");
    expect((await putObject(app, "readyapp", "k", token, "data")).status).toBe(201);
    expect((await fetch(`http://localhost:${app.port}/readyz`)).status).toBe(200);

    fake.stop();
    await fetch(objectUrl(app, "readyapp", "k"), { headers: authHeaders(token) }).catch(() => null);
    expect((await fetch(`http://localhost:${app.port}/readyz`)).status).toBe(503);
  }, 15_000);

  test("export to a directory and import into a fresh service reproduce every object and its metadata", async () => {
    const configA = makeConfig();
    const a = await launch(configA);
    const token = await createBucketViaBot("roundtrip");
    const random = new Uint8Array(1_572_864);
    randomFillSync(random);
    const bodies: Record<string, Uint8Array<ArrayBuffer>> = {
      "a.txt": new TextEncoder().encode("hello export"),
      "dir/random.bin": random,
      empty: new Uint8Array(0),
    };
    for (const [key, body] of Object.entries(bodies)) {
      const headers: Record<string, string> = authHeaders(token, key === "a.txt" ? "text/plain" : "application/octet-stream");
      if (key === "a.txt") headers["x-meta-owner"] = "alice";
      expect((await fetch(objectUrl(a, "roundtrip", key), { method: "PUT", headers, body })).status).toBe(201);
    }

    const work = mkdtempSync(join(tmpdir(), "tgexport-"));
    dirs.push(work);
    const exportDir = join(work, "export");
    const exp = await runCli("export.ts", [exportDir], cliEnv(configA));
    if (exp.code !== 0) throw new Error(exp.err);
    expect(exp.out).toContain("exported 3 objects, 0 errors");

    const fakeB = startFakeTelegram();
    try {
      const adminToken = "import-test-admin-token-0123456789";
      const b = await launch(makeConfig({ apiBase: fakeB.apiBase, adminToken }));
      const importEnv = { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "", ADMIN_TOKEN: adminToken };
      const imp = await runCli("import.ts", [exportDir, `http://localhost:${b.port}`], importEnv);
      if (imp.code !== 0) throw new Error(imp.err);
      expect(imp.out).toContain("imported 3, skipped 0, failed 0");

      const readToken = imp.out.match(TOKEN_RE)?.[0];
      if (readToken === undefined) throw new Error("import printed no bucket password");
      for (const [key, body] of Object.entries(bodies)) {
        const get = await fetch(objectUrl(b, "roundtrip", key), { headers: { Authorization: `Bearer ${readToken}` } });
        expect(get.status).toBe(200);
        expect(new Uint8Array(await get.arrayBuffer())).toEqual(body);
        if (key === "a.txt") {
          expect(get.headers.get("x-meta-owner")).toBe("alice");
          expect(get.headers.get("content-type")).toBe("text/plain");
        }
      }

      const again = await runCli("import.ts", [exportDir, `http://localhost:${b.port}`], importEnv);
      expect(again.code).toBe(0);
      expect(again.out).toContain("imported 0, skipped 3, failed 0");
    } finally {
      fakeB.stop();
    }
  }, 60_000);
});
