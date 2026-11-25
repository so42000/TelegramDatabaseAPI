import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

export interface ObjectRow {
  bucket: string;
  key: string;
  objectId: string;
  messageId: number;
  fileId: string;
  contentType: string;
  size: number;
  sha256: string;
  updatedAt: number;
}

export interface BucketRow {
  name: string;
  tokenHash: string;
  createdAt: number;
}

export interface IndexSnapshot {
  v: 1;
  createdAt: number;
  buckets: BucketRow[];
  objects: ObjectRow[];
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, token_hash TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS objects (
  bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
  key TEXT NOT NULL, object_id TEXT NOT NULL, message_id INTEGER NOT NULL, file_id TEXT NOT NULL,
  content_type TEXT NOT NULL, size INTEGER NOT NULL, sha256 TEXT NOT NULL, updated_at INTEGER NOT NULL,
  PRIMARY KEY (bucket, key));
CREATE TABLE IF NOT EXISTS pending_deletes (message_id INTEGER PRIMARY KEY, queued_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
`;

const OBJECT_COLS = `bucket, key, object_id AS objectId, message_id AS messageId, file_id AS fileId,
  content_type AS contentType, size, sha256, updated_at AS updatedAt`;

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
      this.db
        .query(
          "INSERT OR IGNORE INTO pending_deletes (message_id, queued_at) SELECT message_id, ? FROM objects WHERE bucket = ?",
        )
        .run(Date.now(), name);
      this.db.query("DELETE FROM buckets WHERE name = ?").run(name);
      return count.n;
    })();
  }

  getObject(bucket: string, key: string): ObjectRow | null {
    return this.db
      .query(`SELECT ${OBJECT_COLS} FROM objects WHERE bucket = ? AND key = ?`)
      .get(bucket, key) as ObjectRow | null;
  }

  // Returns the replaced row (its message is queued for deletion), or null.
  upsertObject(row: ObjectRow): ObjectRow | null {
    return this.db.transaction(() => {
      const prev = this.getObject(row.bucket, row.key);
      this.db
        .query(
          `INSERT OR REPLACE INTO objects (bucket, key, object_id, message_id, file_id, content_type, size, sha256, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
        );
      if (prev) {
        this.db
          .query("INSERT OR IGNORE INTO pending_deletes (message_id, queued_at) VALUES (?, ?)")
          .run(prev.messageId, Date.now());
      }
      return prev;
    })();
  }

  // Returns the deleted row (its message is queued for deletion), or null.
  deleteObject(bucket: string, key: string): ObjectRow | null {
    return this.db.transaction(() => {
      const prev = this.getObject(bucket, key);
      if (!prev) return null;
      this.db.query("DELETE FROM objects WHERE bucket = ? AND key = ?").run(bucket, key);
      this.db
        .query("INSERT OR IGNORE INTO pending_deletes (message_id, queued_at) VALUES (?, ?)")
        .run(prev.messageId, Date.now());
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

  loadSnapshot(s: IndexSnapshot): void {
    this.db.transaction(() => {
      const insBucket = this.db.query(
        "INSERT INTO buckets (name, token_hash, created_at) VALUES (?, ?, ?)",
      );
      for (const b of s.buckets) insBucket.run(b.name, b.tokenHash, b.createdAt);
      const insObject = this.db.query(
        `INSERT INTO objects (bucket, key, object_id, message_id, file_id, content_type, size, sha256, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
        );
      }
    })();
  }

  close(): void {
    this.db.close();
  }
}

export function openDb(path: string): IndexDb {
  if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
  const db = new Database(path, { create: true });
  db.exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;");
  db.exec(SCHEMA);
  return new IndexDb(db);
}
