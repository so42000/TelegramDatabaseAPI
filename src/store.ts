import type { IndexDb, ObjectRow } from "./db";
import { decryptBlob, encryptBlob, hashToken, newBucketToken, sha256Hex, tokenMatches } from "./crypto";
import { SerialQueue, type TgMessage, type TelegramClient } from "./telegram";

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
export class QueueFullError extends Error {
  name = "QueueFullError";
}
export class NoSuchBucketError extends Error {
  name = "NoSuchBucketError";
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

// Serializes operations on the same key so concurrent PUTs cannot leak messages.
export class KeyedLock {
  private readonly tails = new Map<string, Promise<void>>();

  run<T>(k: string, fn: () => Promise<T>): Promise<T> {
    const job = (this.tails.get(k) ?? Promise.resolve()).then(fn);
    const tail = job.then(
      () => undefined,
      () => undefined,
    );
    this.tails.set(k, tail);
    void tail.then(() => {
      if (this.tails.get(k) === tail) this.tails.delete(k);
    });
    return job;
  }
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

export interface Dirtyable {
  markDirty(): void;
}

export interface StoreDeps {
  db: IndexDb;
  tg: TelegramClient;
  channelQueue: SerialQueue;
  channelId: string;
  key: CryptoKey;
  snapshot: Dirtyable;
  queueMax: number;
}

export class Store {
  private readonly locks = new KeyedLock();

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
    const { db, tg, channelQueue, channelId, key: masterKey, snapshot, queueMax } = this.deps;
    if (body.byteLength > MAX_OBJECT_BYTES) throw new TooLargeError("object too large");
    if (!db.getBucket(bucket)) throw new NoSuchBucketError("no such bucket");
    if (channelQueue.depth >= queueMax) throw new QueueFullError("channel queue full");

    return this.locks.run(`${bucket}\u0000${key}`, async () => {
      const objectId = crypto.randomUUID();
      const enc = await encryptBlob(masterKey, objectId, body);
      const form = new FormData();
      form.set("chat_id", channelId);
      form.set("document", new Blob([enc]), `${objectId}.bin`);
      form.set("caption", objectId);
      form.set("disable_notification", "true");

      const msg = await channelQueue.run(() => tg.callMultipart<TgMessage>("sendDocument", form));
      if (!msg.document) throw new Error("sendDocument returned no document");

      const row: ObjectRow = {
        bucket,
        key,
        objectId,
        messageId: msg.message_id,
        fileId: msg.document.file_id,
        contentType: contentType.slice(0, 255),
        size: body.byteLength,
        sha256: sha256Hex(body),
        updatedAt: Date.now(),
      };

      let previous: ObjectRow | null;
      try {
        previous = db.upsertObject(row);
      } catch (err) {
        try {
          await tg.call("deleteMessage", { chat_id: channelId, message_id: msg.message_id });
        } catch (delErr) {
          console.error(`orphan message ${msg.message_id} kept: ${String(delErr)}`);
        }
        throw err;
      }
      snapshot.markDirty();
      return { created: previous === null, meta: toMeta(row) };
    });
  }

  async get(bucket: string, key: string): Promise<{ meta: ObjectMeta; body: Uint8Array<ArrayBuffer> } | null> {
    const row = this.deps.db.getObject(bucket, key);
    if (!row) return null;
    const enc = await this.deps.tg.downloadFile(row.fileId);
    const body = await decryptBlob(this.deps.key, row.objectId, enc);
    if (sha256Hex(body) !== row.sha256) throw new IntegrityError("sha256 mismatch");
    return { meta: toMeta(row), body };
  }

  delete(bucket: string, key: string): Promise<boolean> {
    return this.locks.run(`${bucket}\u0000${key}`, async () => {
      const prev = this.deps.db.deleteObject(bucket, key);
      if (!prev) return false;
      this.deps.snapshot.markDirty();
      return true;
    });
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
