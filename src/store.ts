import type { IndexDb, ObjectRow } from "./db";
import { decryptBlob, encryptBlob, hashToken, newBucketToken, sha256Hex, tokenMatches } from "./crypto";
import { NoSuchBucketError, type Batcher, type Dirtyable } from "./batcher";
import type { TelegramClient } from "./telegram";

// Stays below the 20 MB getFile cap even with the 29-byte encryption overhead.
export const MAX_OBJECT_BYTES = 19 * 1024 * 1024;
export const BUCKET_NAME_RE = /^[a-z0-9][a-z0-9_-]{0,62}$/;

const MAX_KEY_BYTES = 1024;
const DEFAULT_LIST_LIMIT = 100;
const MAX_LIST_LIMIT = 1000;

export function keyError(key: string): string | null {
  const bytes = new TextEncoder().encode(key).byteLength;
  if (bytes < 1 || bytes > MAX_KEY_BYTES) return "key must be 1-1024 bytes of UTF-8";
  for (const ch of key) {
    const c = ch.codePointAt(0) ?? 0;
    if (c < 0x20 || c === 0x7f) return "key contains control characters";
  }
  return null;
}

export class TooLargeError extends Error {
  name = "TooLargeError";
}
export class InvalidNameError extends Error {
  name = "InvalidNameError";
}
export class BucketExistsError extends Error {
  name = "BucketExistsError";
}
export class IntegrityError extends Error {
  name = "IntegrityError";
}

export interface ObjectMeta {
  bucket: string;
  key: string;
  size: number;
  contentType: string;
  etag: string;
  updatedAt: string;
}

function toMeta(row: ObjectRow): ObjectMeta {
  return {
    bucket: row.bucket,
    key: row.key,
    size: row.size,
    contentType: row.contentType,
    etag: row.sha256,
    updatedAt: new Date(row.updatedAt).toISOString(),
  };
}

export interface StoreDeps {
  db: IndexDb;
  tg: TelegramClient;
  batcher: Batcher;
  key: CryptoKey;
  snapshot: Dirtyable;
}

export class Store {
  constructor(private readonly deps: StoreDeps) {}

  authorize(bucket: string, token: string): boolean {
    const b = this.deps.db.getBucket(bucket);
    return b !== null && tokenMatches(token, b.tokenHash);
  }

  async put(
    bucket: string,
    key: string,
    body: Uint8Array<ArrayBuffer>,
    contentType: string,
  ): Promise<{ created: boolean; meta: ObjectMeta }> {
    const { db, key: masterKey, batcher } = this.deps;
    if (body.byteLength > MAX_OBJECT_BYTES) throw new TooLargeError("object too large");
    if (!db.getBucket(bucket)) throw new NoSuchBucketError("no such bucket");

    const objectId = crypto.randomUUID();
    const enc = await encryptBlob(masterKey, objectId, body);
    const { created, row } = await batcher.enqueue(
      {
        bucket,
        key,
        objectId,
        contentType: contentType.slice(0, 255),
        size: body.byteLength,
        sha256: sha256Hex(body),
      },
      enc,
    );
    return { created, meta: toMeta(row) };
  }

  async get(bucket: string, key: string): Promise<{ meta: ObjectMeta; body: Uint8Array<ArrayBuffer> } | null> {
    const row = this.deps.db.getObject(bucket, key);
    if (!row) return null;
    const enc = await this.deps.tg.downloadFile(row.fileId, { offset: row.blobOffset, length: row.blobLength });
    const body = await decryptBlob(this.deps.key, row.objectId, enc);
    if (sha256Hex(body) !== row.sha256) throw new IntegrityError("sha256 mismatch");
    return { meta: toMeta(row), body };
  }

  // Concurrent PUT and DELETE on the same key have no guaranteed order.
  delete(bucket: string, key: string): boolean {
    const prev = this.deps.db.deleteObject(bucket, key);
    if (!prev) return false;
    this.deps.snapshot.markDirty();
    return true;
  }

  list(
    bucket: string,
    prefix: string,
    limit: number | undefined,
    cursor: string | null,
  ): { objects: ObjectMeta[]; nextCursor: string | null } {
    const n = Math.min(MAX_LIST_LIMIT, Math.max(1, limit ?? DEFAULT_LIST_LIMIT));
    const rows = this.deps.db.listObjects(bucket, prefix, n + 1, cursor);
    const page = rows.slice(0, n);
    const last = page.at(-1);
    const nextCursor = rows.length > n && last ? last.key : null;
    return { objects: page.map(toMeta), nextCursor };
  }

  createBucket(name: string): string {
    if (!BUCKET_NAME_RE.test(name)) throw new InvalidNameError("invalid bucket name");
    const token = newBucketToken();
    if (!this.deps.db.createBucket(name, hashToken(token), Date.now())) {
      throw new BucketExistsError("bucket already exists");
    }
    this.deps.snapshot.markDirty();
    return token;
  }

  rotateBucket(name: string): string | null {
    const token = newBucketToken();
    if (!this.deps.db.setBucketToken(name, hashToken(token))) return null;
    this.deps.snapshot.markDirty();
    return token;
  }

  // Returns the number of objects removed, or -1 if the bucket does not exist.
  deleteBucket(name: string): number {
    const removed = this.deps.db.deleteBucket(name);
    if (removed >= 0) this.deps.snapshot.markDirty();
    return removed;
  }

  listBuckets() {
    return this.deps.db.listBuckets();
  }
}
