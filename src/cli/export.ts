import { existsSync, mkdirSync, renameSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { loadConfig } from "../config";
import { decryptBlob, importMasterKey, partAad } from "../crypto";
import { openDb, type ObjectRow, type PartRow } from "../db";
import { errorText } from "../log";
import { BotPool } from "../pool";
import { TelegramError } from "../telegram";

const dir = process.argv[2];
if (!dir) {
  console.error("usage: bun run export <dir>");
  process.exit(2);
}
const indexPath = join(dir, "index.ndjson");
if (existsSync(indexPath)) {
  console.error(`export: ${indexPath} already exists`);
  process.exit(2);
}
mkdirSync(dir, { recursive: true });

// Read-only against Telegram and the index DB, so it is safe to run alongside the live service (WAL mode).
// Takeover is a channel write, so it is paced like the service's own channel writes.
const config = loadConfig();
const db = openDb(config.dbPath);
const pool = await BotPool.connect({
  apiBase: config.apiBase,
  tokens: config.botTokens,
  minIntervalMs: config.channelMinIntervalMs,
});
const key = await importMasterKey(config.masterKey);
const index = Bun.file(indexPath).writer();

// Parts of one Telegram document are read with a single download. Rows arrive ordered by first message id,
// so neighbouring objects hit this one-entry cache.
let cached: { messageId: number; bytes: Uint8Array<ArrayBuffer> } | null = null;

async function documentBytes(part: PartRow): Promise<Uint8Array<ArrayBuffer>> {
  if (cached?.messageId === part.messageId) return cached.bytes;
  const lane = pool.byBotId(part.botId);
  let bytes: Uint8Array<ArrayBuffer> | null = null;
  if (lane) {
    try {
      bytes = await lane.tg.downloadFile(part.fileId);
    } catch (err) {
      // 400 means the file id is not valid for this bot (it was stored by another one); fall back to takeover.
      if (!(err instanceof TelegramError && err.code === 400)) throw err;
    }
  }
  if (!bytes) {
    const taken = await pool.takeOver(config.channelId, part.messageId);
    bytes = await taken.lane.tg.downloadFile(taken.fileId);
  }
  cached = { messageId: part.messageId, bytes };
  return bytes;
}

// Streams the plaintext of an object into a temp file, hashing as it goes. The temp file is renamed into place
// only when its hash matches the index, so a partial or corrupt blob never sits at a content-addressed path.
async function materialize(row: ObjectRow, target: string): Promise<void> {
  const tmp = `${target}.tmp`;
  const writer = Bun.file(tmp).writer();
  const hasher = new Bun.CryptoHasher("sha256");
  try {
    for (const part of db.getParts(row.objectId)) {
      const doc = await documentBytes(part);
      const enc = doc.subarray(part.blobOffset, part.blobOffset + part.blobLength);
      const plain = await decryptBlob(key, partAad(row.objectId, part.partNo), enc);
      hasher.update(plain);
      writer.write(plain);
    }
    if (hasher.digest("hex") !== row.sha256) throw new Error("sha256 mismatch");
    await writer.end();
    renameSync(tmp, target);
  } catch (err) {
    await Promise.resolve(writer.end()).catch(() => undefined);
    rmSync(tmp, { force: true });
    throw err;
  }
}

let ok = 0;
let errors = 0;
for (const row of db.exportRows()) {
  try {
    const file = join("objects", row.sha256.slice(0, 2), row.sha256);
    const target = join(dir, file);
    // Content-addressed: an existing file under this hash is already the verified bytes, so it is reused.
    if (!existsSync(target)) {
      mkdirSync(dirname(target), { recursive: true });
      await materialize(row, target);
    }
    index.write(
      JSON.stringify({
        bucket: row.bucket,
        key: row.key,
        contentType: row.contentType,
        size: row.size,
        sha256: row.sha256,
        updatedAt: new Date(row.updatedAt).toISOString(),
        metadata: JSON.parse(row.meta),
        file,
      }) + "\n",
    );
    ok++;
  } catch (err) {
    errors++;
    index.write(JSON.stringify({ bucket: row.bucket, key: row.key, error: errorText(err) }) + "\n");
  }
}

await index.end();
db.close();
console.log(`exported ${ok} objects, ${errors} errors`);
process.exit(errors > 0 ? 1 : 0);
