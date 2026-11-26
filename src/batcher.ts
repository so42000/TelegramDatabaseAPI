import type { CommitResult, IndexDb, ObjectRow } from "./db";
import type { SerialQueue, TelegramClient, TgMessage } from "./telegram";

// Below the 20 MB getFile cap (19,923,968 bytes once the cap is applied to a whole batch).
export const MAX_BATCH_BYTES = 19 * 1024 * 1024 + 1024;

export class QueueFullError extends Error {
  name = "QueueFullError";
}
export class NoSuchBucketError extends Error {
  name = "NoSuchBucketError";
}

export interface Dirtyable {
  markDirty(): void;
}

export type ObjectDraft = Omit<ObjectRow, "messageId" | "fileId" | "blobOffset" | "blobLength" | "updatedAt">;

export interface BatcherDeps {
  db: IndexDb;
  tg: TelegramClient;
  channelQueue: SerialQueue;
  channelId: string;
  snapshot: Dirtyable;
  maxPendingBytes: number;
}

interface PendingItem {
  draft: ObjectDraft;
  enc: Uint8Array<ArrayBuffer>;
  resolve: (result: { created: boolean; row: ObjectRow }) => void;
  reject: (err: unknown) => void;
}

// Group commit: PUTs that arrive while a channel slot is pending are stored in one Telegram
// document. SerialQueue spaces slots CHANNEL_MIN_INTERVAL_MS apart, which is the collection window.
export class Batcher {
  private pending: PendingItem[] = [];
  private pendingBytes = 0;
  // True while a send job sits in channelQueue and has not started yet.
  private scheduled = false;

  constructor(private readonly deps: BatcherDeps) {}

  async enqueue(draft: ObjectDraft, enc: Uint8Array<ArrayBuffer>): Promise<{ created: boolean; row: ObjectRow }> {
    if (this.pendingBytes > 0 && this.pendingBytes + enc.byteLength > this.deps.maxPendingBytes) {
      throw new QueueFullError("channel queue full");
    }
    const { promise, resolve, reject } = Promise.withResolvers<{ created: boolean; row: ObjectRow }>();
    this.pending.push({ draft, enc, resolve, reject });
    this.pendingBytes += enc.byteLength;
    this.schedule();
    return promise;
  }

  private schedule(): void {
    if (this.scheduled || this.pending.length === 0) return;
    this.scheduled = true;
    void this.deps.channelQueue.run(() => this.sendNext());
  }

  // Runs inside a channelQueue job, so it calls tg directly: going through the queue again would deadlock.
  // Never throws; every failure is delivered to the taken items' reject.
  private async sendNext(): Promise<void> {
    this.scheduled = false;
    let count = 0;
    let takenBytes = 0;
    for (const next of this.pending) {
      if (count > 0 && takenBytes + next.enc.byteLength > MAX_BATCH_BYTES) break;
      count++;
      takenBytes += next.enc.byteLength;
    }
    const taken = this.pending.splice(0, count);
    this.pendingBytes -= takenBytes;
    if (taken.length === 0) return;
    this.schedule();

    const { db, tg, channelId, snapshot } = this.deps;
    const offsets: number[] = [];
    let offset = 0;
    for (const item of taken) {
      offsets.push(offset);
      offset += item.enc.byteLength;
    }

    const batchId = crypto.randomUUID();
    const form = new FormData();
    form.set("chat_id", channelId);
    form.set("document", new Blob(taken.map((item) => item.enc)), `${batchId}.bin`);
    form.set("caption", batchId);
    form.set("disable_notification", "true");

    let msg: TgMessage;
    try {
      msg = await tg.callMultipart<TgMessage>("sendDocument", form);
    } catch (err) {
      for (const item of taken) item.reject(err);
      return;
    }
    const document = msg.document;
    if (!document) {
      const err = new Error("sendDocument returned no document");
      for (const item of taken) item.reject(err);
      return;
    }

    const now = Date.now();
    const rows: ObjectRow[] = taken.map((item, i) => ({
      ...item.draft,
      messageId: msg.message_id,
      fileId: document.file_id,
      blobOffset: offsets[i] ?? 0,
      blobLength: item.enc.byteLength,
      updatedAt: now,
    }));

    let results: CommitResult[];
    try {
      results = db.commitBatch(msg.message_id, rows);
    } catch (err) {
      try {
        await tg.call("deleteMessage", { chat_id: channelId, message_id: msg.message_id });
      } catch (delErr) {
        console.error(`orphan message ${msg.message_id} kept: ${String(delErr)}`);
      }
      for (const item of taken) item.reject(err);
      return;
    }
    snapshot.markDirty();

    taken.forEach((item, i) => {
      const row = rows[i];
      const result = results[i];
      if (!row) return item.reject(new Error("batch row missing"));
      if (result === "no_bucket") return item.reject(new NoSuchBucketError("no such bucket"));
      item.resolve({ created: result === "created", row });
    });
  }
}
