import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { BLOB_OVERHEAD } from "./crypto";

export interface ObjectRow {
  bucket: string;
  key: string;
  objectId: string;
  messageId: number;
  fileId: string;
  // Bot whose file_id this is. 0 = not claimed yet (rows written before multi-bot).
  botId: number;
  contentType: string;
  size: number;
  sha256: string;
  // Byte range of this object's encrypted blob inside the Telegram document.
  blobOffset: number;
  blobLength: number;
  updatedAt: number;
}

export interface BucketRow {
  name: string;
  tokenHash: string;
  createdAt: number;
}

export interface IndexSnapshot {
  v: 3;
  createdAt: number;
  buckets: BucketRow[];
  objects: ObjectRow[];
}

export type CommitResult = "created" | "replaced" | "no_bucket";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, token_hash TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS objects (
  bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
  key TEXT NOT NULL, object_id TEXT NOT NULL, message_id INTEGER NOT NULL, file_id TEXT NOT NULL,
  content_type TEXT NOT NULL, size INTEGER NOT NULL, sha256 TEXT NOT NULL, updated_at INTEGER NOT NULL,
  blob_offset INTEGER NOT NULL DEFAULT 0, blob_length INTEGER NOT NULL, bot_id INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket, key));
CREATE TABLE IF NOT EXISTS pending_deletes (message_id INTEGER PRIMARY KEY, queued_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
`;

const OBJECT_COLS = `bucket, key, object_id AS objectId, message_id AS messageId, file_id AS fileId,
  content_type AS contentType, size, sha256, updated_at AS updatedAt,
  blob_offset AS blobOffset, blob_length AS blobLength, bot_id AS botId`;

const BUCKET_COLS = `name, token_hash AS tokenHash, created_at AS createdAt`;

export class IndexDb {
  constructor(private readonly db: Database) {}

  isEmpty(): boolean {
    const row = this.db
      .query("SELECT (SELECT COUNT(*) FROM buckets) + (SELECT COUNT(*) FROM objects) AS n")
      .get() as { n: number };
    return row.n === 0;
  }

  createBucket(name: string, tokenHash: string, now: number): boolean {
    return (
      this.db
        .query("INSERT OR IGNORE INTO buckets (name, token_hash, created_at) VALUES (?, ?, ?)")
        .run(name, tokenHash, now).changes === 1
    );
  }

  getBucket(name: string): BucketRow | null {
    return this.db.query(`SELECT ${BUCKET_COLS} FROM buckets WHERE name = ?`).get(name) as BucketRow | null;
  }

  setBucketToken(name: string, tokenHash: string): boolean {
    return this.db.query("UPDATE buckets SET token_hash = ? WHERE name = ?").run(tokenHash, name).changes === 1;
  }

  listBuckets(): { name: string; createdAt: number; objectCount: number; totalBytes: number }[] {
    return this.db
      .query(
        `SELECT b.name, b.created_at AS createdAt, COUNT(o.key) AS objectCount,
           COALESCE(SUM(o.size), 0) AS totalBytes
         FROM buckets b LEFT JOIN objects o ON o.bucket = b.name
         GROUP BY b.name ORDER BY b.name`,
      )
      .all() as { name: string; createdAt: number; objectCount: number; totalBytes: number }[];
  }

  // Returns the number of objects removed, or -1 if the bucket does not exist.
  deleteBucket(name: string): number {
    return this.db.transaction(() => {
      if (!this.db.query("SELECT 1 FROM buckets WHERE name = ?").get(name)) return -1;
      const count = this.db.query("SELECT COUNT(*) AS n FROM objects WHERE bucket = ?").get(name) as { n: number };
      const ids = (
        this.db.query("SELECT DISTINCT message_id AS id FROM objects WHERE bucket = ?").all(name) as { id: number }[]
      ).map((r) => r.id);
      this.db.query("DELETE FROM buckets WHERE name = ?").run(name);
      const now = Date.now();
      for (const id of ids) this.queueIfUnreferenced(id, now);
      return count.n;
    })();
  }

  getObject(bucket: string, key: string): ObjectRow | null {
    return this.db
      .query(`SELECT ${OBJECT_COLS} FROM objects WHERE bucket = ? AND key = ?`)
      .get(bucket, key) as ObjectRow | null;
  }

  // A file_id from a row no bot has claimed yet, or null.
  unownedFileId(): string | null {
    const row = this.db
      .query("SELECT file_id AS fileId FROM objects WHERE bot_id = 0 LIMIT 1")
      .get() as { fileId: string } | null;
    return row?.fileId ?? null;
  }

  // Only one bot existed before multi-bot, so every unclaimed row is owned by the bot that proved it can read them.
  // Assigns every unclaimed object to botId; returns rows changed.
  claimUnowned(botId: number): number {
    return this.db.query("UPDATE objects SET bot_id = ? WHERE bot_id = 0").run(botId).changes;
  }

  // Points every object stored in messageId at fileId, a file id that belongs to botId. Returns rows changed.
  rebindMessage(messageId: number, botId: number, fileId: string): number {
    return this.db
      .query("UPDATE objects SET file_id = ?, bot_id = ? WHERE message_id = ?")
      .run(fileId, botId, messageId).changes;
  }

  // Applies rows stored in one Telegram message, in order; later rows win over earlier ones
  // with the same key. Results match `rows` by index. Messages that no longer have any
  // referencing object are queued for deletion.
  commitBatch(messageId: number, rows: ObjectRow[]): CommitResult[] {
    return this.db.transaction(() => {
      const touched = new Set<number>();
      const results: CommitResult[] = [];
      for (const row of rows) {
        if (!this.db.query("SELECT 1 FROM buckets WHERE name = ?").get(row.bucket)) {
          results.push("no_bucket");
          continue;
        }
        const prev = this.getObject(row.bucket, row.key);
        this.db
          .query(
            `INSERT OR REPLACE INTO objects (bucket, key, object_id, message_id, file_id, content_type, size, sha256,
               updated_at, blob_offset, blob_length, bot_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          )
          .run(
            row.bucket,
            row.key,
            row.objectId,
            row.messageId,
            row.fileId,
            row.contentType,
            row.size,
            row.sha256,
            row.updatedAt,
            row.blobOffset,
            row.blobLength,
            row.botId,
          );
        if (prev) touched.add(prev.messageId);
        results.push(prev ? "replaced" : "created");
      }
      touched.add(messageId);
      const now = Date.now();
      for (const id of touched) this.queueIfUnreferenced(id, now);
      return results;
    })();
  }

  // Returns the deleted row (its message is queued if unreferenced), or null.
  deleteObject(bucket: string, key: string): ObjectRow | null {
    return this.db.transaction(() => {
      const prev = this.getObject(bucket, key);
      if (!prev) return null;
      this.db.query("DELETE FROM objects WHERE bucket = ? AND key = ?").run(bucket, key);
      this.queueIfUnreferenced(prev.messageId, Date.now());
      return prev;
    })();
  }

  // substr() instead of LIKE so that % and _ in keys need no escaping.
  listObjects(bucket: string, prefix: string, limit: number, cursor: string | null): ObjectRow[] {
    return this.db
      .query(
        `SELECT ${OBJECT_COLS} FROM objects
         WHERE bucket = ? AND key > ? AND substr(key, 1, length(?)) = ?
         ORDER BY key LIMIT ?`,
      )
      .all(bucket, cursor ?? "", prefix, prefix, limit) as ObjectRow[];
  }

  allBuckets(): BucketRow[] {
    return this.db.query(`SELECT ${BUCKET_COLS} FROM buckets ORDER BY name`).all() as BucketRow[];
  }

  allObjects(): ObjectRow[] {
    return this.db.query(`SELECT ${OBJECT_COLS} FROM objects ORDER BY bucket, key`).all() as ObjectRow[];
  }

  pendingDeletes(): number[] {
    return (this.db.query("SELECT message_id FROM pending_deletes").all() as { message_id: number }[]).map(
      (r) => r.message_id,
    );
  }

  removePendingDelete(id: number): void {
    this.db.query("DELETE FROM pending_deletes WHERE message_id = ?").run(id);
  }

  getMeta(k: string): string | null {
    const row = this.db.query("SELECT v FROM meta WHERE k = ?").get(k) as { v: string } | null;
    return row?.v ?? null;
  }

  setMeta(k: string, v: string): void {
    this.db
      .query("INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v")
      .run(k, v);
  }

  loadSnapshot(s: { buckets: BucketRow[]; objects: ObjectRow[] }): void {
    this.db.transaction(() => {
      const insBucket = this.db.query(
        "INSERT INTO buckets (name, token_hash, created_at) VALUES (?, ?, ?)",
      );
      for (const b of s.buckets) insBucket.run(b.name, b.tokenHash, b.createdAt);
      const insObject = this.db.query(
        `INSERT INTO objects (bucket, key, object_id, message_id, file_id, content_type, size, sha256, updated_at,
           blob_offset, blob_length, bot_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      );
      for (const o of s.objects) {
        insObject.run(
          o.bucket,
          o.key,
          o.objectId,
          o.messageId,
          o.fileId,
          o.contentType,
          o.size,
          o.sha256,
          o.updatedAt,
          o.blobOffset,
          o.blobLength,
          o.botId,
        );
      }
    })();
  }

  close(): void {
    this.db.close();
  }

  // A message is deleted only once no object references it. Callers run inside a transaction.
  private queueIfUnreferenced(messageId: number, now: number): void {
    const referenced = this.db.query("SELECT 1 FROM objects WHERE message_id = ? LIMIT 1").get(messageId);
    if (!referenced) {
      this.db
        .query("INSERT OR IGNORE INTO pending_deletes (message_id, queued_at) VALUES (?, ?)")
        .run(messageId, now);
    }
  }
}

// Adds columns missing from an older `objects` table. Legacy rows each own a whole single-object
// document, so offset is 0 and length is the encrypted size; their bot_id stays 0 until claimed.
function migrate(db: Database): void {
  const cols = db.query("PRAGMA table_info(objects)").all() as { name: string }[];
  if (!cols.some((c) => c.name === "blob_offset")) {
    db.transaction(() => {
      db.exec(
        `ALTER TABLE objects ADD COLUMN blob_offset INTEGER NOT NULL DEFAULT 0;
         ALTER TABLE objects ADD COLUMN blob_length INTEGER NOT NULL DEFAULT 0;
         UPDATE objects SET blob_length = size + ${BLOB_OVERHEAD};`,
      );
    })();
  }
  if (!cols.some((c) => c.name === "bot_id")) {
    db.exec("ALTER TABLE objects ADD COLUMN bot_id INTEGER NOT NULL DEFAULT 0");
  }
  db.exec("CREATE INDEX IF NOT EXISTS objects_message_id ON objects(message_id)");
}

export function openDb(path: string): IndexDb {
  if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
  const db = new Database(path, { create: true });
  db.exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;");
  db.exec(SCHEMA);
  migrate(db);
  return new IndexDb(db);
}
