import { createHash } from "node:crypto";
import { log } from "./log";
import type { Batcher } from "./batcher";
import {
  BLOB_OVERHEAD,
  decryptBlob,
  encryptBlob,
  hashToken,
  newBucketToken,
  newKeyId,
  partAad,
  sha256Hex,
} from "./crypto";
import {
  preconditionHolds,
  type IndexDb,
  type KeyRow,
  type KeyScope,
  type ObjectRow,
  type PartRow,
  type Precondition,
} from "./db";
import { isBotLost, laneName, type BotPool } from "./pool";
import { TelegramError } from "./telegram";

// Plaintext bytes per stored part. With the 29-byte encryption overhead it stays below the 20 MB getFile cap.
export const PART_BYTES = 19 * 1024 * 1024;
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
export class NoSuchBucketError extends Error {
  name = "NoSuchBucketError";
}
export class PreconditionFailedError extends Error {
  name = "PreconditionFailedError";
}
export class IntegrityError extends Error {
  name = "IntegrityError";
}

export interface Dirtyable {
  markDirty(): void;
}

export interface ObjectMeta {
  bucket: string;
  key: string;
  size: number;
  contentType: string;
  etag: string;
  updatedAt: string;
  metadata: Record<string, string>;
}

function toMeta(row: ObjectRow): ObjectMeta {
  return {
    bucket: row.bucket,
    key: row.key,
    size: row.size,
    contentType: row.contentType,
    etag: row.sha256,
    updatedAt: new Date(row.updatedAt).toISOString(),
    metadata: JSON.parse(row.meta) as Record<string, string>,
  };
}

function sortedJson(metadata: Record<string, string>): string {
  const entries = Object.entries(metadata).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return JSON.stringify(Object.fromEntries(entries));
}

export interface KeyView {
  id: string;
  scope: KeyScope;
  label: string;
  createdAt: string;
  expiresAt: string | null;
}

export type IssuedKey = KeyView & { token: string };

function toKeyView(row: KeyRow): KeyView {
  return {
    id: row.id,
    scope: row.scope,
    label: row.label,
    createdAt: new Date(row.createdAt).toISOString(),
    expiresAt: row.expiresAt === null ? null : new Date(row.expiresAt).toISOString(),
  };
}

function concat(chunks: Uint8Array[]): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(chunks.reduce((n, c) => n + c.byteLength, 0));
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.byteLength;
  }
  return out;
}

async function* toChunks(source: AsyncIterable<Uint8Array> | Iterable<Uint8Array>): AsyncGenerator<Uint8Array> {
  for await (const chunk of source) yield chunk;
}

// Hands out exact-size slices of a body. A chunk that straddles a slice boundary waits in `carry`.
class ChunkReader {
  private readonly it: AsyncIterator<Uint8Array>;
  private carry: Uint8Array | null = null;
  private done = false;

  constructor(source: AsyncIterable<Uint8Array> | Iterable<Uint8Array>) {
    this.it = toChunks(source);
  }

  async take(n: number): Promise<Uint8Array<ArrayBuffer>> {
    const pieces: Uint8Array[] = [];
    let got = 0;
    while (got < n) {
      let chunk = this.carry;
      this.carry = null;
      if (chunk === null) {
        if (this.done) break;
        const next = await this.it.next();
        if (next.done) {
          this.done = true;
          break;
        }
        chunk = next.value;
      }
      const need = n - got;
      if (chunk.byteLength > need) {
        pieces.push(chunk.subarray(0, need));
        this.carry = chunk.subarray(need);
        got = n;
      } else {
        pieces.push(chunk);
        got += chunk.byteLength;
      }
    }
    return concat(pieces);
  }

  // Stops the source early (an abandoned upload). The source may already have failed; the upload's own error wins.
  async close(): Promise<void> {
    if (this.done) return;
    this.done = true;
    try {
      await this.it.return?.();
    } catch {
      // ignored: see above
    }
  }
}

export class BodyMismatchError extends Error {
  name = "BodyMismatchError";
}

export interface ByteSpan {
  start: number;
  end: number;
}

export interface ReadResult {
  meta: ObjectMeta;
  body: Uint8Array<ArrayBuffer> | ReadableStream<Uint8Array>;
  length: number;
}

interface Covering {
  part: PartRow;
  from: number;
  to: number;
}

export interface StoreDeps {
  db: IndexDb;
  pool: BotPool;
  batcher: Batcher;
  key: CryptoKey;
  snapshot: Dirtyable;
  channelId: string;
  maxObjectBytes: number;
}

export class Store {
  readonly maxObjectBytes: number;

  constructor(private readonly deps: StoreDeps) {
    this.maxObjectBytes = deps.maxObjectBytes;
  }

  private readonly rebinding = new Map<number, Promise<void>>();

  // Parts from before multi-bot support have bot_id 0. They were all stored by one bot: the first
  // configured bot whose getFile accepts their file_id owns them. If none does, they stay unowned.
  async claimUnownedObjects(): Promise<void> {
    const { db, pool, snapshot } = this.deps;
    const probe = db.unownedPart();
    if (probe === null) return;
    for (const lane of pool.lanes) {
      try {
        await lane.tg.call("getFile", { file_id: probe.fileId });
      } catch (err) {
        if (err instanceof TelegramError && err.code === 400) continue;
        throw err;
      }
      const n = db.claimUnowned(lane.botId);
      snapshot.markDirty();
      log.info(`claim: ${n} parts stored before multi-bot support belong to ${laneName(lane)}`);
      return;
    }
    log.info(
      "claim: no configured bot owns the objects stored before multi-bot support; they move to a configured bot on first read",
    );
  }

  // Concurrent reads of one message share one caption edit.
  private rebind(messageId: number): Promise<void> {
    const running = this.rebinding.get(messageId);
    if (running) return running;
    const job = this.takeOver(messageId).finally(() => this.rebinding.delete(messageId));
    this.rebinding.set(messageId, job);
    return job;
  }

  private async takeOver(messageId: number): Promise<void> {
    const { db, pool, channelId, snapshot } = this.deps;
    const { lane, fileId } = await pool.takeOver(channelId, messageId);
    const moved = db.rebindMessage(messageId, lane.botId, fileId);
    snapshot.markDirty();
    log.info(`rebind: message ${messageId} (${moved} parts) now read by ${laneName(lane)}`);
  }

  // Returns the scope of the key that matches the token, or null. Expired keys do not match.
  authorize(bucket: string, token: string): KeyScope | null {
    const row = this.deps.db.keyByTokenHash(hashToken(token), Date.now());
    return row !== null && row.bucket === bucket ? row.scope : null;
  }

  head(bucket: string, key: string): ObjectMeta | null {
    const row = this.deps.db.getObject(bucket, key);
    return row ? toMeta(row) : null;
  }

  async put(
    bucket: string,
    key: string,
    body: AsyncIterable<Uint8Array> | Iterable<Uint8Array>,
    opts: {
      contentType: string;
      metadata: Record<string, string>;
      cond: Precondition | null;
      declaredBytes: number | null;
    },
  ): Promise<{ created: boolean; meta: ObjectMeta }> {
    const { db, key: masterKey, batcher, snapshot } = this.deps;
    const { declaredBytes } = opts;
    if (declaredBytes !== null && declaredBytes > this.maxObjectBytes) throw new TooLargeError("object too large");
    if (!db.getBucket(bucket)) throw new NoSuchBucketError("no such bucket");
    if (!preconditionHolds(db.getObject(bucket, key)?.sha256 ?? null, opts.cond)) {
      throw new PreconditionFailedError("precondition failed");
    }

    const objectId = crypto.randomUUID();
    const reader = new ChunkReader(body);
    const hash = createHash("sha256");
    const stored: Promise<unknown>[] = [];
    const failures: unknown[] = [];
    let total = 0;
    let partNo = 0;
    try {
      for (;;) {
        if (failures.length > 0) throw failures[0];
        const want = declaredBytes === null ? PART_BYTES : Math.min(PART_BYTES, declaredBytes - total);
        const reservation = await batcher.reserve(want + BLOB_OVERHEAD);
        let plain: Uint8Array<ArrayBuffer>;
        try {
          plain = await reader.take(want);
        } catch (err) {
          reservation.release();
          throw err;
        }
        // Part 0 is stored even when empty so that every object has at least one part.
        if (partNo > 0 && plain.byteLength === 0) {
          reservation.release();
          break;
        }
        total += plain.byteLength;
        if (total > this.maxObjectBytes) {
          reservation.release();
          throw new TooLargeError("object too large");
        }
        hash.update(plain);
        let enc: Uint8Array<ArrayBuffer>;
        try {
          enc = await encryptBlob(masterKey, partAad(objectId, partNo), plain);
        } catch (err) {
          reservation.release();
          throw err;
        }
        const part = reservation.submit({ objectId, partNo, size: plain.byteLength }, enc);
        part.catch((err: unknown) => {
          failures.push(err);
        });
        stored.push(part);
        partNo++;
      }
      if (declaredBytes !== null && (total !== declaredBytes || (await reader.take(1)).byteLength > 0)) {
        throw new BodyMismatchError("body length does not match Content-Length");
      }
      await Promise.all(stored);

      const row: ObjectRow = {
        bucket,
        key,
        objectId,
        contentType: opts.contentType.slice(0, 255),
        size: total,
        sha256: hash.digest("hex"),
        meta: sortedJson(opts.metadata),
        updatedAt: Date.now(),
        partCount: partNo,
      };
      const result = db.commitObject(row, opts.cond);
      if (result === "no_bucket") throw new NoSuchBucketError("no such bucket");
      if (result === "precondition_failed") throw new PreconditionFailedError("precondition failed");
      snapshot.markDirty();
      return { created: result === "created", meta: toMeta(row) };
    } catch (err) {
      await reader.close();
      await Promise.allSettled(stored);
      db.abortObject(objectId);
      snapshot.markDirty();
      throw err;
    }
  }

  // Whole-object reads (range null) verify sha256 and fail the stream if it does not match.
  // A range is an inclusive span already checked against the object size by the caller.
  async read(bucket: string, key: string, range: ByteSpan | null): Promise<ReadResult | null> {
    // The object row and its part rows are read in one synchronous step, so an overwrite during the
    // downloads below cannot mix two generations of the object.
    const row = this.deps.db.getObject(bucket, key);
    if (!row) return null;
    const parts = this.deps.db.getParts(row.objectId);
    const start = range?.start ?? 0;
    const end = range?.end ?? row.size - 1;
    const whole = range === null;
    const meta = toMeta(row);
    const length = end - start + 1;

    const covering: Covering[] = [];
    let offset = 0;
    for (const part of parts) {
      const first = offset;
      const last = offset + part.size - 1;
      offset += part.size;
      if (row.size === 0 || (first <= end && last >= start)) {
        covering.push({ part, from: Math.max(start, first) - first, to: Math.min(end, last) - first });
      }
    }
    const [head, ...rest] = covering;
    if (!head) throw new IntegrityError("object parts do not cover its size");
    if (rest.length === 0) {
      const bytes = await this.readPart(head.part);
      const slice = bytes.subarray(head.from, head.to + 1);
      if (whole && sha256Hex(slice) !== row.sha256) throw new IntegrityError("sha256 mismatch");
      return { meta, body: slice, length };
    }

    // The first part is fetched before returning, so a Telegram failure still becomes an HTTP status.
    let eager: Uint8Array<ArrayBuffer> | null = await this.readPart(head.part);
    const hash = whole ? createHash("sha256") : null;
    let index = 0;
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>(
      {
        pull: async (controller) => {
          const item = covering[index];
          index++;
          if (!item) {
            if (hash !== null && hash.digest("hex") !== row.sha256) {
              controller.error(new IntegrityError("sha256 mismatch"));
            } else {
              controller.close();
            }
            return;
          }
          const bytes = eager ?? (await this.readPart(item.part));
          eager = null;
          if (cancelled) return;
          const slice = bytes.subarray(item.from, item.to + 1);
          hash?.update(slice);
          controller.enqueue(slice);
        },
        cancel: () => {
          cancelled = true;
        },
      },
      { highWaterMark: 0 },
    );
    return { meta, body, length };
  }

  private async readPart(part: PartRow): Promise<Uint8Array<ArrayBuffer>> {
    const { db, pool, key: masterKey } = this.deps;
    let current = part;
    for (let attempt = 0; ; attempt++) {
      const lane = pool.byBotId(current.botId);
      if (lane) {
        try {
          const enc = await lane.tg.downloadFile(current.fileId, {
            offset: current.blobOffset,
            length: current.blobLength,
          });
          return await decryptBlob(masterKey, partAad(current.objectId, current.partNo), enc);
        } catch (err) {
          if (!(attempt === 0 && isBotLost(err) && pool.disable(lane, err))) throw err;
        }
      } else if (attempt > 0) {
        throw new Error(`no configured bot can read message ${current.messageId}`);
      }
      await this.rebind(current.messageId);
      const fresh = db.getPart(current.objectId, current.partNo);
      if (!fresh) throw new Error("object changed during read");
      current = fresh;
    }
  }

  // Concurrent PUT and DELETE on the same key have no guaranteed order.
  delete(bucket: string, key: string, cond: Precondition | null): boolean {
    const result = this.deps.db.deleteObject(bucket, key, cond);
    if (result === "precondition_failed") throw new PreconditionFailedError("precondition failed");
    if (result === null) return false;
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

  createBucket(name: string, label: string): IssuedKey {
    if (!BUCKET_NAME_RE.test(name)) throw new InvalidNameError("invalid bucket name");
    const now = Date.now();
    if (!this.deps.db.createBucket(name, now)) throw new BucketExistsError("bucket already exists");
    const issued = this.issueKey(name, "write", label, now, null);
    this.deps.snapshot.markDirty();
    return issued;
  }

  // The previous keys of the bucket stop matching after graceMs. Returns null if the bucket does not exist.
  rotateBucket(name: string, graceMs: number): IssuedKey | null {
    const { db, snapshot } = this.deps;
    if (!db.getBucket(name)) return null;
    const now = Date.now();
    const issued = this.issueKey(name, "write", "rotated", now, null);
    db.expireOtherKeys(name, issued.id, now + graceMs, now);
    snapshot.markDirty();
    return issued;
  }

  // Returns null if the bucket does not exist.
  createKey(bucket: string, scope: KeyScope, label: string, expiresAt: number | null): IssuedKey | null {
    if (!this.deps.db.getBucket(bucket)) return null;
    const issued = this.issueKey(bucket, scope, label, Date.now(), expiresAt);
    this.deps.snapshot.markDirty();
    return issued;
  }

  // Returns null if the bucket does not exist.
  listKeys(bucket: string): KeyView[] | null {
    if (!this.deps.db.getBucket(bucket)) return null;
    return this.deps.db.listKeys(bucket).map(toKeyView);
  }

  revokeKey(bucket: string, id: string): boolean {
    const removed = this.deps.db.deleteKey(bucket, id);
    if (removed) this.deps.snapshot.markDirty();
    return removed;
  }

  private issueKey(bucket: string, scope: KeyScope, label: string, now: number, expiresAt: number | null): IssuedKey {
    const token = newBucketToken();
    const row: KeyRow = {
      id: newKeyId(),
      bucket,
      tokenHash: hashToken(token),
      scope,
      label,
      createdAt: now,
      expiresAt,
    };
    this.deps.db.insertKey(row);
    return { ...toKeyView(row), token };
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
