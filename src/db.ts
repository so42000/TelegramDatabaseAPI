import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { BLOB_OVERHEAD } from "./crypto";

export interface BucketRow {
  name: string;
  createdAt: number;
}

export type KeyScope = "read" | "write";

export interface KeyRow {
  id: string;
  bucket: string;
  tokenHash: string;
  scope: KeyScope;
  label: string;
  createdAt: number;
  expiresAt: number | null;
}

export interface ObjectRow {
  bucket: string;
  key: string;
  objectId: string;
  contentType: string;
  size: number;
  sha256: string;
  meta: string;
  updatedAt: number;
  partCount: number;
}

// One encrypted blob stored in a Telegram message. Parts of several objects can share a message.
export interface PartRow {
  objectId: string;
  partNo: number;
  messageId: number;
  fileId: string;
  botId: number;
  blobOffset: number;
  blobLength: number;
  size: number;
}

// Conditional-request check against the current sha256 (null when the object is absent).
export interface Precondition {
  ifMatch: string[] | null;
  ifNoneMatch: string[] | null;
}

export function preconditionHolds(etag: string | null, cond: Precondition | null): boolean {
  if (!cond) return true;
  if (cond.ifMatch && (etag === null || (!cond.ifMatch.includes("*") && !cond.ifMatch.includes(etag)))) {
    return false;
  }
  if (cond.ifNoneMatch && etag !== null && (cond.ifNoneMatch.includes("*") || cond.ifNoneMatch.includes(etag))) {
    return false;
  }
  return true;
}

export type CommitResult = "created" | "replaced" | "no_bucket" | "precondition_failed";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS bucket_keys (
  id TEXT PRIMARY KEY,
  bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
  token_hash TEXT NOT NULL UNIQUE,
  scope TEXT NOT NULL CHECK (scope IN ('read', 'write')),
  label TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER);
CREATE TABLE IF NOT EXISTS objects (
  bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
  key TEXT NOT NULL, object_id TEXT NOT NULL, content_type TEXT NOT NULL, size INTEGER NOT NULL,
  sha256 TEXT NOT NULL, meta TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL,
  part_count INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (bucket, key));
CREATE UNIQUE INDEX IF NOT EXISTS objects_object_id ON objects(object_id);
CREATE TABLE IF NOT EXISTS object_parts (
  object_id TEXT NOT NULL, part_no INTEGER NOT NULL, message_id INTEGER NOT NULL, file_id TEXT NOT NULL,
  bot_id INTEGER NOT NULL, blob_offset INTEGER NOT NULL, blob_length INTEGER NOT NULL, size INTEGER NOT NULL,
  PRIMARY KEY (object_id, part_no));
CREATE INDEX IF NOT EXISTS object_parts_message_id ON object_parts(message_id);
CREATE TABLE IF NOT EXISTS pending_deletes (message_id INTEGER PRIMARY KEY, queued_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
`;

const OBJECT_COLS = `bucket, key, object_id AS objectId, content_type AS contentType, size, sha256, meta,
  updated_at AS updatedAt, part_count AS partCount`;
const PART_COLS = `object_id AS objectId, part_no AS partNo, message_id AS messageId, file_id AS fileId,
  bot_id AS botId, blob_offset AS blobOffset, blob_length AS blobLength, size`;
const KEY_COLS = `id, bucket, token_hash AS tokenHash, scope, label, created_at AS createdAt,
  expires_at AS expiresAt`;

export class IndexDb {
  constructor(
    private readonly db: Database,
    private readonly path: string,
  ) {}

  isEmpty(): boolean {
    const row = this.db
      .query(
        `SELECT (SELECT COUNT(*) FROM buckets) + (SELECT COUNT(*) FROM bucket_keys)
          + (SELECT COUNT(*) FROM objects) + (SELECT COUNT(*) FROM object_parts) AS n`,
      )
      .get() as { n: number };
    return row.n === 0;
  }

  ping(): boolean {
    try {
      this.db.query("SELECT 1").get();
      return true;
    } catch {
      return false;
    }
  }

  createBucket(name: string, now: number): boolean {
    return this.db.query("INSERT OR IGNORE INTO buckets (name, created_at) VALUES (?, ?)").run(name, now).changes === 1;
  }

  getBucket(name: string): BucketRow | null {
    return this.db.query("SELECT name, created_at AS createdAt FROM buckets WHERE name = ?").get(name) as BucketRow | null;
  }

  listBuckets(): { name: string; createdAt: number; objectCount: number; totalBytes: number }[] {
    return this.db
      .query(
        `SELECT b.name AS name, b.created_at AS createdAt, COUNT(o.key) AS objectCount,
           COALESCE(SUM(o.size), 0) AS totalBytes
         FROM buckets b LEFT JOIN objects o ON o.bucket = b.name
         GROUP BY b.name ORDER BY b.name`,
      )
      .all() as { name: string; createdAt: number; objectCount: number; totalBytes: number }[];
  }

  // Returns the number of objects removed, or -1 if the bucket does not exist.
  deleteBucket(name: string): number {
    return this.db.transaction((): number => {
      if (!this.getBucket(name)) return -1;
      const countRow = this.db.query("SELECT COUNT(*) AS n FROM objects WHERE bucket = ?").get(name) as { n: number };
      const count = countRow.n;
      const ids = this.db
        .query(
          `SELECT DISTINCT p.message_id AS id FROM object_parts p JOIN objects o ON o.object_id = p.object_id
           WHERE o.bucket = ?`,
        )
        .all(name) as { id: number }[];
      this.db
        .query("DELETE FROM object_parts WHERE object_id IN (SELECT object_id FROM objects WHERE bucket = ?)")
        .run(name);
      this.db.query("DELETE FROM buckets WHERE name = ?").run(name); // cascades objects and keys
      this.queueUnreferenced(ids.map((r) => r.id));
      return count;
    })();
  }

  insertKey(row: KeyRow): void {
    this.db
      .query(
        "INSERT INTO bucket_keys (id, bucket, token_hash, scope, label, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      )
      .run(row.id, row.bucket, row.tokenHash, row.scope, row.label, row.createdAt, row.expiresAt);
  }

  keyByTokenHash(tokenHash: string, now: number): KeyRow | null {
    return this.db
      .query(`SELECT ${KEY_COLS} FROM bucket_keys WHERE token_hash = ? AND (expires_at IS NULL OR expires_at > ?)`)
      .get(tokenHash, now) as KeyRow | null;
  }

  listKeys(bucket: string): KeyRow[] {
    return this.db
      .query(`SELECT ${KEY_COLS} FROM bucket_keys WHERE bucket = ? ORDER BY created_at, id`)
      .all(bucket) as KeyRow[];
  }

  deleteKey(bucket: string, id: string): boolean {
    return this.db.query("DELETE FROM bucket_keys WHERE bucket = ? AND id = ?").run(bucket, id).changes > 0;
  }

  // Rotation grace: other keys of the bucket expire at `at`. A non-positive grace removes them now.
  expireOtherKeys(bucket: string, keepId: string, at: number, now: number): void {
    if (at <= now) {
      this.db.query("DELETE FROM bucket_keys WHERE bucket = ? AND id != ?").run(bucket, keepId);
      return;
    }
    this.db
      .query(
        "UPDATE bucket_keys SET expires_at = MIN(COALESCE(expires_at, ?), ?) WHERE bucket = ? AND id != ?",
      )
      .run(at, at, bucket, keepId);
  }

  deleteExpiredKeys(now: number): number {
    return this.db
      .query("DELETE FROM bucket_keys WHERE expires_at IS NOT NULL AND expires_at <= ?")
      .run(now).changes;
  }

  getObject(bucket: string, key: string): ObjectRow | null {
    return this.db
      .query(`SELECT ${OBJECT_COLS} FROM objects WHERE bucket = ? AND key = ?`)
      .get(bucket, key) as ObjectRow | null;
  }

  getParts(objectId: string): PartRow[] {
    return this.db
      .query(`SELECT ${PART_COLS} FROM object_parts WHERE object_id = ? ORDER BY part_no`)
      .all(objectId) as PartRow[];
  }

  getPart(objectId: string, partNo: number): PartRow | null {
    return this.db
      .query(`SELECT ${PART_COLS} FROM object_parts WHERE object_id = ? AND part_no = ?`)
      .get(objectId, partNo) as PartRow | null;
  }

  // Staged parts: written after their Telegram upload, invisible until commitObject links them.
  commitParts(rows: PartRow[]): void {
    this.db.transaction(() => {
      const insert = this.db.query(
        "INSERT INTO object_parts (object_id, part_no, message_id, file_id, bot_id, blob_offset, blob_length, size) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      );
      for (const r of rows) {
        insert.run(r.objectId, r.partNo, r.messageId, r.fileId, r.botId, r.blobOffset, r.blobLength, r.size);
      }
    })();
  }

  commitObject(row: ObjectRow, cond: Precondition | null): CommitResult {
    return this.db.transaction((): CommitResult => {
      if (!this.getBucket(row.bucket)) return "no_bucket";
      const prev = this.getObject(row.bucket, row.key);
      if (!preconditionHolds(prev?.sha256 ?? null, cond)) return "precondition_failed";
      const prevIds = prev ? this.partMessageIds(prev.objectId) : [];
      if (prev) this.db.query("DELETE FROM object_parts WHERE object_id = ?").run(prev.objectId);
      this.db
        .query(
          `INSERT OR REPLACE INTO objects (bucket, key, object_id, content_type, size, sha256, meta, updated_at, part_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          row.bucket,
          row.key,
          row.objectId,
          row.contentType,
          row.size,
          row.sha256,
          row.meta,
          row.updatedAt,
          row.partCount,
        );
      this.queueUnreferenced(prevIds);
      return prev ? "replaced" : "created";
    })();
  }

  // Drops the staged parts of an upload that failed before commitObject.
  abortObject(objectId: string): void {
    this.db.transaction(() => {
      if (this.db.query("SELECT 1 FROM objects WHERE object_id = ?").get(objectId)) return;
      const ids = this.partMessageIds(objectId);
      this.db.query("DELETE FROM object_parts WHERE object_id = ?").run(objectId);
      this.queueUnreferenced(ids);
    })();
  }

  // Startup cleanup: staged parts left behind by an interrupted upload. Returns the parts removed.
  dropStagedParts(): number {
    return this.db.transaction((): number => {
      const where = "object_id NOT IN (SELECT object_id FROM objects)";
      const ids = this.db.query(`SELECT DISTINCT message_id AS id FROM object_parts WHERE ${where}`).all() as {
        id: number;
      }[];
      const removed = this.db.query(`DELETE FROM object_parts WHERE ${where}`).run().changes;
      this.queueUnreferenced(ids.map((r) => r.id));
      return removed;
    })();
  }

  // Returns the removed row, null when absent, or "precondition_failed".
  deleteObject(bucket: string, key: string, cond: Precondition | null): ObjectRow | null | "precondition_failed" {
    return this.db.transaction((): ObjectRow | null | "precondition_failed" => {
      const prev = this.getObject(bucket, key);
      if (!preconditionHolds(prev?.sha256 ?? null, cond)) return "precondition_failed";
      if (!prev) return null;
      const ids = this.partMessageIds(prev.objectId);
      this.db.query("DELETE FROM object_parts WHERE object_id = ?").run(prev.objectId);
      this.db.query("DELETE FROM objects WHERE bucket = ? AND key = ?").run(bucket, key);
      this.queueUnreferenced(ids);
      return prev;
    })();
  }

  listObjects(bucket: string, prefix: string, limit: number, cursor: string | null): ObjectRow[] {
    return this.db
      .query(
        `SELECT ${OBJECT_COLS} FROM objects WHERE bucket = ? AND substr(key, 1, length(?)) = ? AND key > ?
         ORDER BY key LIMIT ?`,
      )
      .all(bucket, prefix, prefix, cursor ?? "", limit) as ObjectRow[];
  }

  // Every object with the lowest message id among its parts (null for empty objects). Ordering keeps objects
  // that share a Telegram document adjacent, so the exporter can reuse one download.
  exportRows(): IterableIterator<ObjectRow & { firstMessageId: number | null }> {
    return this.db
      .query(
        `SELECT ${OBJECT_COLS}, first_message_id AS firstMessageId FROM objects
         LEFT JOIN (SELECT object_id, MIN(message_id) AS first_message_id FROM object_parts GROUP BY object_id) AS fp
         USING (object_id) ORDER BY firstMessageId`,
      )
      .iterate() as IterableIterator<ObjectRow & { firstMessageId: number | null }>;
  }

  // Legacy single-bot era: parts stored before a bot id was recorded. Returns one such part to probe with.
  unownedPart(): { messageId: number; fileId: string } | null {
    return this.db
      .query("SELECT message_id AS messageId, file_id AS fileId FROM object_parts WHERE bot_id = 0 LIMIT 1")
      .get() as { messageId: number; fileId: string } | null;
  }

  claimUnowned(botId: number): number {
    return this.db.query("UPDATE object_parts SET bot_id = ? WHERE bot_id = 0").run(botId).changes;
  }

  rebindMessage(messageId: number, botId: number, fileId: string): number {
    return this.db
      .query("UPDATE object_parts SET bot_id = ?, file_id = ? WHERE message_id = ?")
      .run(botId, fileId, messageId).changes;
  }

  pendingDeletes(): number[] {
    return (
      this.db.query("SELECT message_id AS id FROM pending_deletes ORDER BY queued_at, message_id").all() as {
        id: number;
      }[]
    ).map((r) => r.id);
  }

  removePendingDelete(id: number): void {
    this.db.query("DELETE FROM pending_deletes WHERE message_id = ?").run(id);
  }

  queueDeletes(ids: number[]): void {
    this.db.transaction(() => {
      const now = Date.now();
      for (const id of ids) {
        this.db.query("INSERT OR IGNORE INTO pending_deletes (message_id, queued_at) VALUES (?, ?)").run(id, now);
      }
    })();
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

  stats(): { objects: number; bytes: number; pendingDeletes: number } {
    return this.db
      .query(
        `SELECT (SELECT COUNT(*) FROM objects) AS objects,
           COALESCE((SELECT SUM(size) FROM objects), 0) AS bytes,
           (SELECT COUNT(*) FROM pending_deletes) AS pendingDeletes`,
      )
      .get() as { objects: number; bytes: number; pendingDeletes: number };
  }

  beginRestore(): RestoreWriter {
    return new RestoreWriter(this.db);
  }

  openSnapshotReader(): SnapshotReader {
    if (this.path === ":memory:") throw new Error("snapshots need a file-backed DB");
    return new SnapshotReader(this.path);
  }

  close(): void {
    this.db.close();
  }

  private partMessageIds(objectId: string): number[] {
    return (
      this.db.query("SELECT DISTINCT message_id AS id FROM object_parts WHERE object_id = ?").all(objectId) as {
        id: number;
      }[]
    ).map((r) => r.id);
  }

  // A message is deleted only once no part row references it: one batch holds parts of several objects.
  private queueUnreferenced(ids: number[]): void {
    const now = Date.now();
    for (const id of new Set(ids)) {
      const ref = this.db.query("SELECT 1 FROM object_parts WHERE message_id = ? LIMIT 1").get(id);
      if (!ref) {
        this.db.query("INSERT OR IGNORE INTO pending_deletes (message_id, queued_at) VALUES (?, ?)").run(id, now);
      }
    }
  }
}

// Writes a whole snapshot inside one transaction; the caller commits only after every part is read.
export class RestoreWriter {
  constructor(private readonly db: Database) {
    db.exec("BEGIN IMMEDIATE");
  }

  bucket(row: BucketRow): void {
    this.db.query("INSERT INTO buckets (name, created_at) VALUES (?, ?)").run(row.name, row.createdAt);
  }

  key(row: KeyRow): void {
    this.db
      .query(
        "INSERT INTO bucket_keys (id, bucket, token_hash, scope, label, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      )
      .run(row.id, row.bucket, row.tokenHash, row.scope, row.label, row.createdAt, row.expiresAt);
  }

  object(row: ObjectRow): void {
    this.db
      .query(
        `INSERT INTO objects (bucket, key, object_id, content_type, size, sha256, meta, updated_at, part_count)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        row.bucket,
        row.key,
        row.objectId,
        row.contentType,
        row.size,
        row.sha256,
        row.meta,
        row.updatedAt,
        row.partCount,
      );
  }

  part(row: PartRow): void {
    this.db
      .query(
        "INSERT INTO object_parts (object_id, part_no, message_id, file_id, bot_id, blob_offset, blob_length, size) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      )
      .run(row.objectId, row.partNo, row.messageId, row.fileId, row.botId, row.blobOffset, row.blobLength, row.size);
  }

  commit(): void {
    this.db.exec("COMMIT");
  }

  rollback(): void {
    this.db.exec("ROLLBACK");
  }
}

// A read transaction over the whole index, so paged reads see one consistent state while writes continue.
export class SnapshotReader {
  private readonly db: Database;
  readonly pendingDeletes: number[];

  constructor(path: string) {
    this.db = new Database(path, { readonly: true });
    try {
      this.db.exec("BEGIN");
      this.pendingDeletes = (
        this.db.query("SELECT message_id AS id FROM pending_deletes ORDER BY queued_at, message_id").all() as {
          id: number;
        }[]
      ).map((r) => r.id);
    } catch (err) {
      this.close();
      throw err;
    }
  }

  buckets(): BucketRow[] {
    return this.db.query("SELECT name, created_at AS createdAt FROM buckets ORDER BY name").all() as BucketRow[];
  }

  keys(): KeyRow[] {
    return this.db.query(`SELECT ${KEY_COLS} FROM bucket_keys ORDER BY id`).all() as KeyRow[];
  }

  *objectPages(pageRows: number): Generator<ObjectRow[]> {
    let bucket = "";
    let key = "";
    for (;;) {
      const rows = this.db
        .query(`SELECT ${OBJECT_COLS} FROM objects WHERE (bucket, key) > (?, ?) ORDER BY bucket, key LIMIT ?`)
        .all(bucket, key, pageRows) as ObjectRow[];
      if (rows.length === 0) return;
      yield rows;
      const last = rows.at(-1);
      if (!last) return;
      bucket = last.bucket;
      key = last.key;
    }
  }

  // Joins objects so staged parts (uploads not yet linked) are excluded from the snapshot.
  *partPages(pageRows: number): Generator<PartRow[]> {
    let objectId = "";
    let partNo = -1;
    for (;;) {
      const rows = this.db
        .query(
          `SELECT p.object_id AS objectId, p.part_no AS partNo, p.message_id AS messageId, p.file_id AS fileId,
             p.bot_id AS botId, p.blob_offset AS blobOffset, p.blob_length AS blobLength, p.size AS size
           FROM object_parts p JOIN objects o ON o.object_id = p.object_id
           WHERE (p.object_id, p.part_no) > (?, ?) ORDER BY p.object_id, p.part_no LIMIT ?`,
        )
        .all(objectId, partNo, pageRows) as PartRow[];
      if (rows.length === 0) return;
      yield rows;
      const last = rows.at(-1);
      if (!last) return;
      objectId = last.objectId;
      partNo = last.partNo;
    }
  }

  close(): void {
    try {
      if (this.db.inTransaction) this.db.exec("COMMIT");
    } finally {
      this.db.close();
    }
  }
}

// Adds what an older schema lacks and moves per-object blob data into object_parts.
// Legacy rows each own one whole single-object document: offset 0, length = encrypted size.
function migrate(db: Database): void {
  const version = (db.query("PRAGMA user_version").get() as { user_version: number }).user_version;
  if (version >= 4) return;
  db.transaction(() => {
    migrateObjects(db);
    migrateBuckets(db);
    db.exec("PRAGMA user_version = 4");
  })();
}

function columns(db: Database, table: string): Set<string> {
  return new Set((db.query(`PRAGMA table_info(${table})`).all() as { name: string }[]).map((c) => c.name));
}

function migrateObjects(db: Database): void {
  const cols = columns(db, "objects");
  if (!cols.has("message_id")) return;
  if (!cols.has("blob_offset")) {
    db.exec("ALTER TABLE objects ADD COLUMN blob_offset INTEGER NOT NULL DEFAULT 0");
  }
  if (!cols.has("blob_length")) {
    db.exec(
      `ALTER TABLE objects ADD COLUMN blob_length INTEGER NOT NULL DEFAULT 0;
       UPDATE objects SET blob_length = size + ${BLOB_OVERHEAD};`,
    );
  }
  if (!cols.has("bot_id")) {
    db.exec("ALTER TABLE objects ADD COLUMN bot_id INTEGER NOT NULL DEFAULT 0");
  }
  db.exec(`
    INSERT INTO object_parts (object_id, part_no, message_id, file_id, bot_id, blob_offset, blob_length, size)
      SELECT object_id, 0, message_id, file_id, bot_id, blob_offset, blob_length, size FROM objects;
    DROP INDEX IF EXISTS objects_message_id;
    ALTER TABLE objects DROP COLUMN message_id;
    ALTER TABLE objects DROP COLUMN file_id;
    ALTER TABLE objects DROP COLUMN bot_id;
    ALTER TABLE objects DROP COLUMN blob_offset;
    ALTER TABLE objects DROP COLUMN blob_length;
    ALTER TABLE objects ADD COLUMN meta TEXT NOT NULL DEFAULT '{}';
    ALTER TABLE objects ADD COLUMN part_count INTEGER NOT NULL DEFAULT 1;
  `);
}

// Each legacy bucket token becomes one write key, labelled "migrated".
function migrateBuckets(db: Database): void {
  if (!columns(db, "buckets").has("token_hash")) return;
  db.exec(`
    INSERT INTO bucket_keys (id, bucket, token_hash, scope, label, created_at, expires_at)
      SELECT 'key_' || lower(hex(randomblob(8))), name, token_hash, 'write', 'migrated', created_at, NULL FROM buckets;
    ALTER TABLE buckets DROP COLUMN token_hash;
  `);
}

export function openDb(path: string): IndexDb {
  if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
  const db = new Database(path, { create: true });
  db.exec("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;");
  db.exec(SCHEMA);
  migrate(db);
  return new IndexDb(db, path);
}
