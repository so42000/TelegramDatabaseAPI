import { errorText, log } from "./log";
import { BLOB_OVERHEAD, DecryptError, decryptBlob, encryptBlob, newKeyId } from "./crypto";
import type { BucketRow, IndexDb, KeyRow, ObjectRow, PartRow, SnapshotReader } from "./db";
import { TelegramError, type TelegramClient, type TgChatFullInfo, type TgMessage } from "./telegram";
import { isBotLost, type BotLane, type BotPool } from "./pool";
import { snapshotFlushes } from "./metrics";

// v4 layout: a manifest message is pinned in the channel and lists part messages. Each part is
// one encrypted run of length-prefixed gzip frames; each frame holds NDJSON rows.
export const MANIFEST_CAPTION = "tgstore-index-manifest v1";
const MANIFEST_AAD = "tgstore-index-manifest";
const PART_CAPTION = "tgstore-index-part v1";
const SNAPSHOT_FILENAME = "index-snapshot.bin";
const SNAPSHOT_META = "snapshot_message_id";
const PARTS_META = "snapshot_part_ids";
const FORMAT_META = "snapshot_format";
const FORMAT = "4";
// Telegram getFile caps downloads at 20 MB; a frame above this could not be restored from one part.
const MAX_FRAME_BYTES = 19 * 1024 * 1024;

// v1-v3 snapshots: one encrypted gzip JSON document, pinned as-is.
export const LEGACY_SNAPSHOT_CAPTION = "tgstore-index-snapshot v1";
export const LEGACY_SNAPSHOT_AAD = "tgstore-index-snapshot";

// v1 predates blob ranges; v2 predates bot_id (the owner of each file_id is unknown, so 0 until claimed).
interface LegacyBucket {
  name: string;
  tokenHash: string;
  createdAt: number;
}
interface LegacyObjectV1 {
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
interface LegacyObjectV2 extends LegacyObjectV1 {
  blobOffset: number;
  blobLength: number;
}
interface LegacyObjectV3 extends LegacyObjectV2 {
  botId: number;
}
type LegacyPayload =
  | { v: 1; buckets: LegacyBucket[]; objects: LegacyObjectV1[] }
  | { v: 2; buckets: LegacyBucket[]; objects: LegacyObjectV2[] }
  | { v: 3; buckets: LegacyBucket[]; objects: LegacyObjectV3[] };

export interface SnapshotTuning {
  pageRows: number;
  partMaxBytes: number;
}

const DEFAULT_TUNING: SnapshotTuning = { pageRows: 5000, partMaxBytes: 18 * 1024 * 1024 };

export interface SnapshotDeps {
  db: IndexDb;
  pool: BotPool;
  channelId: string;
  key: CryptoKey;
  intervalMs: number;
  tuning?: SnapshotTuning;
}

export type RestoreResult = "restored" | "empty-start" | "local";

interface PartRef {
  messageId: number;
  fileId: string;
  botId: number;
}

interface Counts {
  buckets: number;
  keys: number;
  objects: number;
  parts: number;
}

interface Manifest {
  v: 4;
  snapshotId: string;
  createdAt: number;
  parts: PartRef[];
  counts: Counts;
}

type SnapshotRecord =
  | ({ t: "b" } & BucketRow)
  | ({ t: "k" } & KeyRow)
  | ({ t: "o" } & ObjectRow)
  | ({ t: "p" } & PartRow);

export function snapshotPartAad(snapshotId: string, index: number): string {
  return `tgstore-index-part:${snapshotId}:${index}`;
}

function legacyObjects(payload: LegacyPayload): LegacyObjectV3[] {
  switch (payload.v) {
    case 1:
      return payload.objects.map((o) => ({ ...o, blobOffset: 0, blobLength: o.size + BLOB_OVERHEAD, botId: 0 }));
    case 2:
      return payload.objects.map((o) => ({ ...o, botId: 0 }));
    case 3:
      return payload.objects;
    default:
      throw new Error("restore: unsupported snapshot version");
  }
}

async function openPayload(key: CryptoKey, aad: string, enc: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer>> {
  try {
    return await decryptBlob(key, aad, enc);
  } catch (err) {
    if (err instanceof DecryptError) throw new Error("restore: snapshot decrypt failed (wrong MASTER_KEY?)");
    throw err;
  }
}

function encodeFrame(lines: string[]): Uint8Array<ArrayBuffer> {
  const gz = Bun.gzipSync(new TextEncoder().encode(lines.join("\n")));
  if (gz.byteLength > MAX_FRAME_BYTES) throw new Error("snapshot frame too large");
  const framed = new Uint8Array(4 + gz.byteLength);
  new DataView(framed.buffer).setUint32(0, gz.byteLength);
  framed.set(gz, 4);
  return framed;
}

function* frameLines(plain: Uint8Array<ArrayBuffer>): Generator<string> {
  const view = new DataView(plain.buffer, plain.byteOffset, plain.byteLength);
  let off = 0;
  while (off < plain.byteLength) {
    if (off + 4 > plain.byteLength) throw new Error("restore: snapshot part is truncated");
    const len = view.getUint32(off);
    off += 4;
    if (off + len > plain.byteLength) throw new Error("restore: snapshot part is truncated");
    const text = new TextDecoder().decode(Bun.gunzipSync(plain.subarray(off, off + len)));
    off += len;
    for (const line of text.split("\n")) if (line !== "") yield line;
  }
}

function concat(chunks: Uint8Array<ArrayBuffer>[], total: number): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.byteLength;
  }
  return out;
}

function countsMatch(a: Counts, b: Counts): boolean {
  return a.buckets === b.buckets && a.keys === b.keys && a.objects === b.objects && a.parts === b.parts;
}

// Persists the index as an encrypted manifest pinned in the storage channel, plus part messages.
// Mutations call markDirty(); flushes are chained, so concurrent callers share one ordered sequence.
export class Snapshot {
  private dirty = false;
  private generation = 0;
  private chain: Promise<void> = Promise.resolve();
  private timer: Timer | undefined;
  private flushing = false;
  private lastSuccessAt: number | null = null;
  private readonly tuning: SnapshotTuning;

  constructor(private readonly deps: SnapshotDeps) {
    this.tuning = deps.tuning ?? DEFAULT_TUNING;
  }

  markDirty(): void {
    this.generation++;
    this.dirty = true;
  }

  status(): { dirty: boolean; lastSuccessAt: number | null } {
    return { dirty: this.dirty, lastSuccessAt: this.lastSuccessAt };
  }

  flush(): Promise<void> {
    const run = this.chain.then(() => this.doFlush());
    this.chain = run;
    return run;
  }

  // Restores the index only when the local DB is empty. A pinned snapshot that cannot be
  // read throws, so an empty index never overwrites a good snapshot.
  async restoreIfEmpty(): Promise<RestoreResult> {
    const { db, pool, channelId } = this.deps;
    if (!db.isEmpty()) {
      this.dirty = db.getMeta(SNAPSHOT_META) === null || db.getMeta(FORMAT_META) !== FORMAT;
      return "local";
    }

    const { tg } = pool.primary();
    const chat = await tg.call<TgChatFullInfo>("getChat", { chat_id: channelId });
    const pinned = chat.pinned_message;
    if (!pinned) {
      log.info("restore: no snapshot pinned, starting empty");
      return "empty-start";
    }
    const doc = pinned.document;
    if (!doc || (pinned.caption !== MANIFEST_CAPTION && pinned.caption !== LEGACY_SNAPSHOT_CAPTION)) {
      throw new Error(
        "restore: pinned message in storage channel is not an index snapshot; unpin it and re-pin the snapshot message",
      );
    }
    const enc = await tg.downloadFile(doc.file_id);
    if (pinned.caption === LEGACY_SNAPSHOT_CAPTION) return this.restoreLegacy(enc, pinned.message_id);
    return this.restoreManifest(enc, pinned.message_id);
  }

  start(): void {
    this.timer = setInterval(() => {
      if (!this.flushing) void this.flush();
    }, this.deps.intervalMs);
  }

  async stop(): Promise<void> {
    clearInterval(this.timer);
    this.timer = undefined;
    await this.flush();
  }

  private async restoreLegacy(enc: Uint8Array<ArrayBuffer>, messageId: number): Promise<RestoreResult> {
    const { db, key } = this.deps;
    const plain = await openPayload(key, LEGACY_SNAPSHOT_AAD, enc);
    const payload = JSON.parse(new TextDecoder().decode(Bun.gunzipSync(plain))) as LegacyPayload;
    const objects = legacyObjects(payload);

    const writer = db.beginRestore();
    try {
      for (const b of payload.buckets) {
        writer.bucket({ name: b.name, createdAt: b.createdAt });
        writer.key({
          id: newKeyId(),
          bucket: b.name,
          tokenHash: b.tokenHash,
          scope: "write",
          label: "migrated",
          createdAt: b.createdAt,
          expiresAt: null,
        });
      }
      for (const o of objects) {
        writer.object({
          bucket: o.bucket,
          key: o.key,
          objectId: o.objectId,
          contentType: o.contentType,
          size: o.size,
          sha256: o.sha256,
          meta: "{}",
          updatedAt: o.updatedAt,
          partCount: 1,
        });
        writer.part({
          objectId: o.objectId,
          partNo: 0,
          messageId: o.messageId,
          fileId: o.fileId,
          botId: o.botId,
          blobOffset: o.blobOffset,
          blobLength: o.blobLength,
          size: o.size,
        });
      }
      writer.commit();
    } catch (err) {
      writer.rollback();
      throw err;
    }
    db.setMeta(SNAPSHOT_META, String(messageId));
    // The legacy pin is not format 4; flush rewrites it as a manifest.
    this.markDirty();
    log.info(`restore: restored ${payload.buckets.length} buckets, ${objects.length} objects from snapshot`);
    return "restored";
  }

  private async restoreManifest(enc: Uint8Array<ArrayBuffer>, messageId: number): Promise<RestoreResult> {
    const { db, key } = this.deps;
    const manifest = JSON.parse(new TextDecoder().decode(await openPayload(key, MANIFEST_AAD, enc))) as Manifest;
    if (manifest.v !== 4) throw new Error("restore: unsupported snapshot version");

    const counts: Counts = { buckets: 0, keys: 0, objects: 0, parts: 0 };
    const writer = db.beginRestore();
    try {
      for (const [i, ref] of manifest.parts.entries()) {
        const plain = await this.fetchPart(ref, snapshotPartAad(manifest.snapshotId, i));
        for (const line of frameLines(plain)) {
          const rec = JSON.parse(line) as SnapshotRecord;
          switch (rec.t) {
            case "b":
              writer.bucket(rec);
              counts.buckets++;
              break;
            case "k":
              writer.key(rec);
              counts.keys++;
              break;
            case "o":
              writer.object(rec);
              counts.objects++;
              break;
            case "p":
              writer.part(rec);
              counts.parts++;
              break;
          }
        }
      }
      if (!countsMatch(counts, manifest.counts)) throw new Error("restore: snapshot is incomplete");
      writer.commit();
    } catch (err) {
      writer.rollback();
      throw err;
    }
    db.setMeta(SNAPSHOT_META, String(messageId));
    db.setMeta(PARTS_META, JSON.stringify(manifest.parts.map((p) => p.messageId)));
    db.setMeta(FORMAT_META, FORMAT);
    log.info(`restore: restored ${counts.buckets} buckets, ${counts.objects} objects from snapshot`);
    return "restored";
  }

  // The owner bot may be gone or removed from the pool; then any live bot takes the message over.
  private async fetchPart(ref: PartRef, aad: string): Promise<Uint8Array<ArrayBuffer>> {
    const { pool, channelId, key } = this.deps;
    let enc: Uint8Array<ArrayBuffer> | null = null;
    const owner = pool.byBotId(ref.botId);
    if (owner) {
      try {
        enc = await owner.tg.downloadFile(ref.fileId);
      } catch (err) {
        if (!(err instanceof TelegramError && err.code === 400)) throw err;
      }
    }
    if (enc === null) {
      const taken = await pool.takeOver(channelId, ref.messageId);
      enc = await taken.lane.tg.downloadFile(taken.fileId);
    }
    return openPayload(key, aad, enc);
  }

  private async doFlush(): Promise<void> {
    this.flushing = true;
    try {
      await this.writeSnapshot();
    } finally {
      this.flushing = false;
    }
  }

  private async writeSnapshot(): Promise<void> {
    const { db, pool, key } = this.deps;
    if (db.deleteExpiredKeys(Date.now()) > 0) this.markDirty();
    if (!this.dirty) return;
    const gen = this.generation;
    const lane = pool.primary();
    // Part and manifest messages uploaded by this flush and not yet referenced by a pinned manifest.
    // On failure they are queued for deletion; once the manifest references them they must survive.
    const uploaded: number[] = [];
    let reader: SnapshotReader | undefined;
    try {
      // The reader is one read transaction: the parts and the pending-delete list describe the same state.
      reader = db.openSnapshotReader();
      const pending = reader.pendingDeletes;
      const snapshotId = crypto.randomUUID();
      const { parts, counts } = await this.uploadParts(lane, reader, snapshotId, uploaded);
      const manifest: Manifest = { v: 4, snapshotId, createdAt: Date.now(), parts, counts };
      const blob = await encryptBlob(key, MANIFEST_AAD, new TextEncoder().encode(JSON.stringify(manifest)));

      // Editing keeps the pinned message id; a 400 (sent by another bot, or gone) pins a new manifest.
      const existing = db.getMeta(SNAPSHOT_META);
      const edited = existing !== null && (await this.editManifest(lane, Number(existing), blob));
      const manifestId = edited ? Number(existing) : await this.pinManifest(lane, blob, uploaded);
      uploaded.length = 0;

      // Old parts are unreferenced now; their ids are durable in pending_deletes before any delete is attempted.
      const stale = JSON.parse(db.getMeta(PARTS_META) ?? "[]") as number[];
      if (existing !== null && Number(existing) !== manifestId) stale.push(Number(existing));
      db.setMeta(SNAPSHOT_META, String(manifestId));
      db.setMeta(PARTS_META, JSON.stringify(parts.map((p) => p.messageId)));
      db.setMeta(FORMAT_META, FORMAT);
      db.queueDeletes(stale);

      if (this.generation === gen) this.dirty = false;
      this.lastSuccessAt = Date.now();
      snapshotFlushes.inc({ outcome: "ok" });
      for (const id of new Set([...pending, ...stale])) await this.deletePending(lane.tg, id);
    } catch (err) {
      if (uploaded.length > 0) db.queueDeletes(uploaded);
      if (isBotLost(err)) pool.disable(lane, err);
      snapshotFlushes.inc({ outcome: "error" });
      log.error("snapshot flush failed", { err: errorText(err) });
    } finally {
      reader?.close();
    }
  }

  // Streams the snapshot as frames grouped into parts of at most partMaxBytes. A part always
  // holds at least one frame, so a frame larger than partMaxBytes becomes a part of its own.
  private async uploadParts(
    lane: BotLane,
    reader: SnapshotReader,
    snapshotId: string,
    uploaded: number[],
  ): Promise<{ parts: PartRef[]; counts: Counts }> {
    const { key } = this.deps;
    const { pageRows, partMaxBytes } = this.tuning;
    const parts: PartRef[] = [];
    const counts: Counts = { buckets: 0, keys: 0, objects: 0, parts: 0 };
    let frames: Uint8Array<ArrayBuffer>[] = [];
    let bytes = 0;

    const seal = async (): Promise<void> => {
      const plain = concat(frames, bytes);
      const enc = await encryptBlob(key, snapshotPartAad(snapshotId, parts.length), plain);
      parts.push(await this.uploadPart(lane, enc, uploaded));
      frames = [];
      bytes = 0;
    };
    const add = async (frame: Uint8Array<ArrayBuffer>): Promise<void> => {
      if (bytes > 0 && bytes + frame.byteLength > partMaxBytes) await seal();
      frames.push(frame);
      bytes += frame.byteLength;
    };

    const buckets = reader.buckets();
    const keys = reader.keys();
    counts.buckets = buckets.length;
    counts.keys = keys.length;
    const head = [
      ...buckets.map((b) => JSON.stringify({ t: "b", ...b })),
      ...keys.map((k) => JSON.stringify({ t: "k", ...k })),
    ];
    if (head.length > 0) await add(encodeFrame(head));
    for (const page of reader.objectPages(pageRows)) {
      counts.objects += page.length;
      await add(encodeFrame(page.map((o) => JSON.stringify({ t: "o", ...o }))));
    }
    for (const page of reader.partPages(pageRows)) {
      counts.parts += page.length;
      await add(encodeFrame(page.map((p) => JSON.stringify({ t: "p", ...p }))));
    }
    if (bytes > 0) await seal();
    return { parts, counts };
  }

  private async uploadPart(lane: BotLane, enc: Uint8Array<ArrayBuffer>, uploaded: number[]): Promise<PartRef> {
    const form = new FormData();
    form.set("chat_id", this.deps.channelId);
    form.set("document", new Blob([enc]), SNAPSHOT_FILENAME);
    form.set("caption", PART_CAPTION);
    form.set("disable_notification", "true");
    const msg = await lane.queue.run(() => lane.tg.callMultipart<TgMessage>("sendDocument", form));
    uploaded.push(msg.message_id);
    if (!msg.document) throw new Error("snapshot part upload returned no document");
    return { messageId: msg.message_id, fileId: msg.document.file_id, botId: lane.botId };
  }

  // Returns false when Telegram refuses the edit (the message was sent by another bot, or is gone).
  private async editManifest(lane: BotLane, messageId: number, blob: Uint8Array<ArrayBuffer>): Promise<boolean> {
    const form = new FormData();
    form.set("chat_id", this.deps.channelId);
    form.set("message_id", String(messageId));
    form.set("media", JSON.stringify({ type: "document", media: "attach://snapshot", caption: MANIFEST_CAPTION }));
    form.set("snapshot", new Blob([blob]), SNAPSHOT_FILENAME);
    try {
      await lane.queue.run(() => lane.tg.callMultipart("editMessageMedia", form));
      return true;
    } catch (err) {
      if (err instanceof TelegramError && err.code === 400) return false;
      throw err;
    }
  }

  private async pinManifest(lane: BotLane, blob: Uint8Array<ArrayBuffer>, uploaded: number[]): Promise<number> {
    const { channelId } = this.deps;
    const form = new FormData();
    form.set("chat_id", channelId);
    form.set("document", new Blob([blob]), SNAPSHOT_FILENAME);
    form.set("caption", MANIFEST_CAPTION);
    form.set("disable_notification", "true");
    const msg = await lane.queue.run(() => lane.tg.callMultipart<TgMessage>("sendDocument", form));
    uploaded.push(msg.message_id);
    await lane.queue.run(() =>
      lane.tg.call("pinChatMessage", { chat_id: channelId, message_id: msg.message_id, disable_notification: true }),
    );
    return msg.message_id;
  }

  // Deletes run outside the channel queue (deleteMessage bypasses it). A 400 means the
  // message is already gone, so the pending row is dropped; other errors keep it for the next flush.
  private async deletePending(tg: TelegramClient, id: number): Promise<void> {
    const { db, channelId } = this.deps;
    try {
      await tg.call("deleteMessage", { chat_id: channelId, message_id: id });
      db.removePendingDelete(id);
    } catch (err) {
      if (err instanceof TelegramError && err.code === 400) {
        log.error(`orphan message ${id} kept`, { err: err.description });
        db.removePendingDelete(id);
        return;
      }
      log.error(`delete message ${id} failed, will retry`, { err: errorText(err) });
    }
  }
}
