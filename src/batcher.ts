import { errorText, log } from "./log";
import type { IndexDb, PartRow } from "./db";
import type { TgMessage } from "./telegram";
import { isBotLost, type BotLane, type BotPool } from "./pool";

// Below the 20 MB getFile cap (19,923,968 bytes once the cap is applied to a whole batch).
export const MAX_BATCH_BYTES = 19 * 1024 * 1024 + 1024;
const QUEUE_WAIT_MS = 30_000;

export class QueueFullError extends Error {
  name = "QueueFullError";
}

export interface PartDraft {
  objectId: string;
  partNo: number;
  size: number;
}

export interface BatcherDeps {
  db: IndexDb;
  pool: BotPool;
  channelId: string;
  maxPendingBytes: number;
}

// Single use. submit() turns the reserved bytes into queued bytes; release() returns them unused.
export interface Reservation {
  submit(part: PartDraft, enc: Uint8Array<ArrayBuffer>): Promise<PartRow>;
  release(): void;
}

interface PendingItem {
  part: PartDraft;
  enc: Uint8Array<ArrayBuffer>;
  resolve: (row: PartRow) => void;
  reject: (err: unknown) => void;
}

interface Waiter {
  bytes: number;
  grant: () => void;
}

// Group commit: a part goes out at once on the bot whose slot is free first. Parts that arrive while every
// bot's slot is busy wait in `pending` and go out as one document. Each bot's SerialQueue spaces its own writes.
export class Batcher {
  private pending: PendingItem[] = [];
  // Bytes held by granted reservations plus bytes queued in `pending`.
  private used = 0;
  private waiters: Waiter[] = [];
  // True while a send job sits in a bot's queue and has not started yet.
  private scheduled = false;

  constructor(private readonly deps: BatcherDeps) {}

  reserve(bytes: number): Promise<Reservation> {
    if (this.waiters.length === 0 && this.fits(bytes)) return Promise.resolve(this.grant(bytes));
    const { promise, resolve, reject } = Promise.withResolvers<Reservation>();
    const timer = setTimeout(() => {
      this.waiters = this.waiters.filter((w) => w !== waiter);
      reject(new QueueFullError("channel queue full"));
    }, QUEUE_WAIT_MS);
    const waiter: Waiter = {
      bytes,
      grant: () => {
        clearTimeout(timer);
        resolve(this.grant(bytes));
      },
    };
    this.waiters.push(waiter);
    return promise;
  }

  stats(): { queuedBytes: number; queuedParts: number } {
    let queuedBytes = 0;
    for (const item of this.pending) queuedBytes += item.enc.byteLength;
    return { queuedBytes, queuedParts: this.pending.length };
  }

  // An empty batcher always admits one reservation, so a part larger than maxPendingBytes can still be stored.
  private fits(bytes: number): boolean {
    return this.used === 0 || this.used + bytes <= this.deps.maxPendingBytes;
  }

  private grant(bytes: number): Reservation {
    this.used += bytes;
    let open = true;
    return {
      submit: (part, enc) => {
        if (!open) throw new Error("reservation already settled");
        open = false;
        this.used += enc.byteLength - bytes;
        const { promise, resolve, reject } = Promise.withResolvers<PartRow>();
        this.pending.push({ part, enc, resolve, reject });
        this.schedule();
        return promise;
      },
      release: () => {
        if (!open) return;
        open = false;
        this.used -= bytes;
        this.wake();
      },
    };
  }

  // Grants waiting reservations in FIFO order while they fit.
  private wake(): void {
    let next = this.waiters[0];
    while (next && this.fits(next.bytes)) {
      this.waiters.shift();
      next.grant();
      next = this.waiters[0];
    }
  }

  private schedule(): void {
    if (this.scheduled || this.pending.length === 0) return;
    const lane = this.deps.pool.pickWriter();
    this.scheduled = true;
    void lane.queue.run(() => this.sendNext(lane));
  }

  // Runs inside a lane.queue job, so it calls lane.tg directly: going through the queue again would deadlock.
  // Never throws; every failure is delivered to the taken items' reject.
  private async sendNext(lane: BotLane): Promise<void> {
    this.scheduled = false;
    if (lane.disabled) {
      this.schedule();
      return;
    }
    let count = 0;
    let takenBytes = 0;
    for (const next of this.pending) {
      if (count > 0 && takenBytes + next.enc.byteLength > MAX_BATCH_BYTES) break;
      count++;
      takenBytes += next.enc.byteLength;
    }
    const taken = this.pending.splice(0, count);
    if (taken.length === 0) return;
    this.used -= takenBytes;
    this.schedule();
    this.wake();

    const { db, channelId } = this.deps;
    const batchId = crypto.randomUUID();
    const form = new FormData();
    form.set("chat_id", channelId);
    form.set("document", new Blob(taken.map((item) => item.enc)), `${batchId}.bin`);
    form.set("caption", batchId);
    form.set("disable_notification", "true");

    let msg: TgMessage;
    try {
      msg = await lane.tg.callMultipart<TgMessage>("sendDocument", form);
    } catch (err) {
      // concat, not unshift(...): a batch can hold hundreds of thousands of items
      if (isBotLost(err) && this.deps.pool.disable(lane, err)) {
        this.pending = taken.concat(this.pending);
        this.used += takenBytes;
        this.schedule();
        return;
      }
      for (const item of taken) item.reject(err);
      return;
    }
    const messageId = msg.message_id;
    const document = msg.document;
    if (!document) {
      const err = new Error("sendDocument returned no document");
      for (const item of taken) item.reject(err);
      return;
    }

    let offset = 0;
    const rows: PartRow[] = taken.map((item) => {
      const row: PartRow = {
        objectId: item.part.objectId,
        partNo: item.part.partNo,
        messageId,
        fileId: document.file_id,
        botId: lane.botId,
        blobOffset: offset,
        blobLength: item.enc.byteLength,
        size: item.part.size,
      };
      offset += item.enc.byteLength;
      return row;
    });

    try {
      db.commitParts(rows);
    } catch (err) {
      try {
        await lane.tg.call("deleteMessage", { chat_id: channelId, message_id: messageId });
      } catch (delErr) {
        log.error(`orphan message ${messageId} kept`, { err: errorText(delErr) });
      }
      for (const item of taken) item.reject(err);
      return;
    }

    taken.forEach((item, i) => {
      const row = rows[i];
      if (!row) return item.reject(new Error("batch row missing"));
      item.resolve(row);
    });
  }
}
