import { DecryptError, decryptBlob, encryptBlob } from "./crypto";
import type { IndexDb, IndexSnapshot } from "./db";
import { SerialQueue, TelegramError, type TelegramClient, type TgChatFullInfo, type TgMessage } from "./telegram";

export const SNAPSHOT_CAPTION = "tgstore-index-snapshot v1";
export const SNAPSHOT_AAD = "tgstore-index-snapshot";
const SNAPSHOT_FILENAME = "index-snapshot.bin";
const SNAPSHOT_META = "snapshot_message_id";

export interface SnapshotDeps {
  db: IndexDb;
  tg: TelegramClient;
  channelQueue: SerialQueue;
  channelId: string;
  key: CryptoKey;
  intervalMs: number;
}

export type RestoreResult = "restored" | "empty-start" | "local";

function errorText(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// Persists the index as an encrypted document pinned in the storage channel.
// Mutations call markDirty(); flushes are chained, so concurrent callers share one ordered sequence.
export class Snapshot {
  private dirty = false;
  private generation = 0;
  private chain: Promise<void> = Promise.resolve();
  private timer: Timer | undefined;

  constructor(private readonly deps: SnapshotDeps) {}

  markDirty(): void {
    this.generation++;
    this.dirty = true;
  }

  flush(): Promise<void> {
    const run = this.chain.then(() => this.doFlush());
    this.chain = run;
    return run;
  }

  // Restores the index only when the local DB is empty. A pinned snapshot that cannot be
  // read throws, so an empty index never overwrites a good snapshot.
  async restoreIfEmpty(): Promise<RestoreResult> {
    const { db, tg, channelId, key } = this.deps;
    if (!db.isEmpty()) {
      this.dirty = db.getMeta(SNAPSHOT_META) === null;
      return "local";
    }

    const chat = await tg.call<TgChatFullInfo>("getChat", { chat_id: channelId });
    const pinned = chat.pinned_message;
    if (!pinned) {
      console.log("restore: no snapshot pinned, starting empty");
      return "empty-start";
    }
    if (pinned.caption !== SNAPSHOT_CAPTION || !pinned.document) {
      throw new Error(
        "restore: pinned message in storage channel is not an index snapshot; unpin it and re-pin the snapshot message",
      );
    }

    const enc = await tg.downloadFile(pinned.document.file_id);
    let snap: IndexSnapshot;
    try {
      const plain = await decryptBlob(key, SNAPSHOT_AAD, enc);
      snap = JSON.parse(new TextDecoder().decode(Bun.gunzipSync(plain))) as IndexSnapshot;
    } catch (err) {
      if (err instanceof DecryptError) throw new Error("restore: snapshot decrypt failed (wrong MASTER_KEY?)");
      throw err;
    }
    if (snap.v !== 1) throw new Error("restore: unsupported snapshot version");

    db.loadSnapshot(snap);
    db.setMeta(SNAPSHOT_META, String(pinned.message_id));
    console.log(`restore: restored ${snap.buckets.length} buckets, ${snap.objects.length} objects from snapshot`);
    return "restored";
  }

  start(): void {
    this.timer = setInterval(() => void this.flush(), this.deps.intervalMs);
  }

  async stop(): Promise<void> {
    clearInterval(this.timer);
    this.timer = undefined;
    await this.flush();
  }

  private async doFlush(): Promise<void> {
    if (!this.dirty) return;
    const { db, tg, channelQueue, channelId, key } = this.deps;
    const gen = this.generation;
    try {
      // Both reads happen before the snapshot is built. Rows in `pending` are already gone
      // from `objects`, so the new snapshot cannot reference them.
      const pending = db.pendingDeletes();
      const payload = Bun.gzipSync(
        new TextEncoder().encode(
          JSON.stringify({
            v: 1,
            createdAt: Date.now(),
            buckets: db.allBuckets(),
            objects: db.allObjects(),
          } satisfies IndexSnapshot),
        ),
      );
      const enc = await encryptBlob(key, SNAPSHOT_AAD, payload);
      const blob = new Blob([enc]);

      let edited = false;
      const existing = db.getMeta(SNAPSHOT_META);
      if (existing !== null) {
        const form = new FormData();
        form.set("chat_id", channelId);
        form.set("message_id", existing);
        form.set("media", JSON.stringify({ type: "document", media: "attach://snapshot", caption: SNAPSHOT_CAPTION }));
        form.set("snapshot", blob, SNAPSHOT_FILENAME);
        try {
          await channelQueue.run(() => tg.callMultipart("editMessageMedia", form));
          edited = true;
        } catch (err) {
          if (!(err instanceof TelegramError && err.code === 400)) throw err;
        }
      }
      if (!edited) {
        const form = new FormData();
        form.set("chat_id", channelId);
        form.set("document", blob, SNAPSHOT_FILENAME);
        form.set("caption", SNAPSHOT_CAPTION);
        form.set("disable_notification", "true");
        const msg = await channelQueue.run(() => tg.callMultipart<TgMessage>("sendDocument", form));
        await channelQueue.run(() =>
          tg.call("pinChatMessage", { chat_id: channelId, message_id: msg.message_id, disable_notification: true }),
        );
        db.setMeta(SNAPSHOT_META, String(msg.message_id));
      }

      if (this.generation === gen) this.dirty = false;
      for (const id of pending) await this.deletePending(id);
    } catch (err) {
      console.error(`snapshot flush failed: ${errorText(err)}`);
    }
  }

  // Deletes run outside the channel queue (deleteMessage bypasses it). A 400 means the
  // message is already gone, so the pending row is dropped; other errors keep it for the next flush.
  private async deletePending(id: number): Promise<void> {
    const { db, tg, channelId } = this.deps;
    try {
      await tg.call("deleteMessage", { chat_id: channelId, message_id: id });
      db.removePendingDelete(id);
    } catch (err) {
      if (err instanceof TelegramError && err.code === 400) {
        console.error(`orphan message ${id} kept: ${err.description}`);
        db.removePendingDelete(id);
        return;
      }
      console.error(`delete message ${id} failed, will retry: ${errorText(err)}`);
    }
  }
}
