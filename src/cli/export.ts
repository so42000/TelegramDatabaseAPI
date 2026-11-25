import { loadConfig } from "../config";
import { decryptBlob, importMasterKey, sha256Hex } from "../crypto";
import { openDb } from "../db";
import { TelegramClient } from "../telegram";

const out = process.argv[2];
if (!out) {
  console.error("usage: bun run export <out.ndjson>");
  process.exit(2);
}

// Read-only against Telegram and the index DB, so it is safe to run alongside the live service (WAL mode).
const config = loadConfig();
const db = openDb(config.dbPath);
const tg = new TelegramClient({ apiBase: config.apiBase, token: config.botToken });
const key = await importMasterKey(config.masterKey);
const writer = Bun.file(out).writer();

let ok = 0;
let errors = 0;
for (const row of db.allObjects()) {
  try {
    const body = await decryptBlob(key, row.objectId, await tg.downloadFile(row.fileId));
    if (sha256Hex(body) !== row.sha256) throw new Error("sha256 mismatch");
    writer.write(
      JSON.stringify({
        bucket: row.bucket,
        key: row.key,
        contentType: row.contentType,
        size: row.size,
        sha256: row.sha256,
        updatedAt: new Date(row.updatedAt).toISOString(),
        data: Buffer.from(body).toString("base64"),
      }) + "\n",
    );
    ok++;
  } catch (err) {
    errors++;
    writer.write(
      JSON.stringify({ bucket: row.bucket, key: row.key, error: err instanceof Error ? err.message : String(err) }) +
        "\n",
    );
  }
}

await writer.end();
db.close();
console.log(`exported ${ok} objects, ${errors} errors`);
process.exit(errors > 0 ? 1 : 0);
