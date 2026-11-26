import { loadConfig } from "../config";
import { decryptBlob, importMasterKey, sha256Hex } from "../crypto";
import { openDb, type ObjectRow } from "../db";
import { BotPool } from "../pool";

const out = process.argv[2];
if (!out) {
  console.error("usage: bun run export <out.ndjson>");
  process.exit(2);
}

// Read-only against Telegram and the index DB, so it is safe to run alongside the live service (WAL mode).
const config = loadConfig();
const db = openDb(config.dbPath);
const pool = await BotPool.connect({ apiBase: config.apiBase, tokens: config.botTokens, minIntervalMs: 0 });
const key = await importMasterKey(config.masterKey);
const writer = Bun.file(out).writer();

function errorLine(row: ObjectRow, err: unknown): string {
  return JSON.stringify({ bucket: row.bucket, key: row.key, error: err instanceof Error ? err.message : String(err) }) + "\n";
}

// Several objects can share one Telegram document, so rows are grouped by owning bot and file id (file ids are
// per bot) and each file is downloaded once.
const byFile = new Map<string, ObjectRow[]>();
for (const row of db.allObjects()) {
  const groupKey = `${row.botId}:${row.fileId}`;
  const group = byFile.get(groupKey);
  if (group) group.push(row);
  else byFile.set(groupKey, [row]);
}

let ok = 0;
let errors = 0;
for (const rows of byFile.values()) {
  const [first] = rows;
  if (!first) continue;
  let file: Uint8Array<ArrayBuffer>;
  try {
    const lane = pool.byBotId(first.botId);
    if (!lane) {
      throw new Error(
        `stored by bot ${first.botId}, which is not in TELEGRAM_BOT_TOKENS; start the service and GET it once to move it to a configured bot`,
      );
    }
    file = await lane.tg.downloadFile(first.fileId);
  } catch (err) {
    for (const row of rows) {
      errors++;
      writer.write(errorLine(row, err));
    }
    continue;
  }
  for (const row of rows) {
    try {
      const enc = file.slice(row.blobOffset, row.blobOffset + row.blobLength);
      const body = await decryptBlob(key, row.objectId, enc);
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
      writer.write(errorLine(row, err));
    }
  }
}

await writer.end();
db.close();
console.log(`exported ${ok} objects, ${errors} errors`);
process.exit(errors > 0 ? 1 : 0);
