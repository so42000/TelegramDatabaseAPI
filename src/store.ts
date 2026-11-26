import type { IndexDb, ObjectRow } from "./db";
import { decryptBlob, encryptBlob, hashToken, newBucketToken, sha256Hex, tokenMatches } from "./crypto";
import { NoSuchBucketError, type Batcher, type Dirtyable } from "./batcher";
import { isBotLost, laneName, type BotPool } from "./pool";
import { TelegramError, type TgMessage } from "./telegram";

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
  pool: BotPool;
  batcher: Batcher;
  key: CryptoKey;
  snapshot: Dirtyable;
  channelId: string;
}

export class Store {
  constructor(private readonly deps: StoreDeps) {}

  private readonly rebinding = new Map<number, Promise<void>>();

  // Objects from before multi-bot support have bot_id 0. They were all stored by one bot: the first
  // configured bot whose getFile accepts their file_id owns them. If none does, they stay unowned.
  async claimUnownedObjects(): Promise<void> {
    const { db, pool, snapshot } = this.deps;
    const fileId = db.unownedFileId();
    if (fileId === null) return;
    for (const lane of pool.lanes) {
      try {
        await lane.tg.call("getFile", { file_id: fileId });
      } catch (err) {
        if (err instanceof TelegramError && err.code === 400) continue;
        throw err;
      }
      const n = db.claimUnowned(lane.botId);
      snapshot.markDirty();
      console.log(`claim: ${n} objects stored before multi-bot support belong to ${laneName(lane)}`);
      return;
    }
    console.log("claim: no configured bot owns the objects stored before multi-bot support; they move to a configured bot on first read");
  }

  // Concurrent reads of one message share one caption edit.
  private rebind(messageId: number): Promise<void> {
    const running = this.rebinding.get(messageId);
    if (running) return running;
    const job = this.takeOver(messageId).finally(() => this.rebinding.delete(messageId));
    this.rebinding.set(messageId, job);
    return job;
  }

  // File ids work only for the bot that received them. Editing a batch message's caption (allowed for any
  // channel admin with Edit rights) returns the message with a file_id for the editing bot, which then owns
  // every object in it. Captions are random UUIDs, so the edit reveals nothing.
  private async takeOver(messageId: number): Promise<void> {
    const { db, pool, channelId, snapshot } = this.deps;
    for (;;) {
      const lane = pool.pickWriter();
      let msg: TgMessage;
      try {
        msg = await lane.queue.run(() =>
          lane.tg.call<TgMessage>("editMessageCaption", {
            chat_id: channelId,
            message_id: messageId,
            caption: crypto.randomUUID(),
          }),
        );
      } catch (err) {
        if (isBotLost(err) && pool.disable(lane, err)) continue;
        throw err;
      }
      if (!msg.document) throw new Error(`message ${messageId} has no document`);
      const moved = db.rebindMessage(messageId, lane.botId, msg.document.file_id);
      snapshot.markDirty();
      console.log(`rebind: message ${messageId} (${moved} objects) now read by ${laneName(lane)}`);
      return;
    }
  }

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
    for (let attempt = 0; ; attempt++) {
      const row = this.deps.db.getObject(bucket, key);
      if (!row) return null;
      const lane = this.deps.pool.byBotId(row.botId);
      if (!lane) {
        if (attempt > 0) throw new Error(`no configured bot can read message ${row.messageId}`);
        await this.rebind(row.messageId);
        continue;
      }
      let enc: Uint8Array<ArrayBuffer>;
      try {
        enc = await lane.tg.downloadFile(row.fileId, { offset: row.blobOffset, length: row.blobLength });
      } catch (err) {
        if (attempt === 0 && isBotLost(err) && this.deps.pool.disable(lane, err)) {
          await this.rebind(row.messageId);
          continue;
        }
        throw err;
      }
      const body = await decryptBlob(this.deps.key, row.objectId, enc);
      if (sha256Hex(body) !== row.sha256) throw new IntegrityError("sha256 mismatch");
      return { meta: toMeta(row), body };
    }
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
