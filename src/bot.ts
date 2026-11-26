import { errorText, log } from "./log";
import {
  BucketExistsError,
  InvalidNameError,
  keyError,
  NoSuchBucketError,
  Store,
} from "./store";
import type { Snapshot } from "./snapshot";
import type { TelegramClient, TgMessage, TgUpdate } from "./telegram";
import { isBotLost } from "./pool";

const MIB = 1024 * 1024;
const MAX_BOT_UPLOAD_BYTES = 19 * MIB;
const MAX_BOT_SEND_BYTES = 48 * MIB;
const LS_PAGE = 50;
const POLL_TIMEOUT_S = 50;
const RETRY_DELAY_MS = 5000;
const ROTATE_GRACE_MS = 86_400_000;

const HELP = [
  "<b>Commands</b>",
  "/newbucket &lt;name&gt; - create a bucket, shows its password once",
  "/rotate &lt;name&gt; [now] - new password; old ones expire in 24 h (or now)",
  "/buckets - list buckets",
  "/ls &lt;bucket&gt; [prefix] - list keys",
  "/get &lt;bucket&gt; &lt;key&gt; - send an object as a file",
  "/rm &lt;bucket&gt; &lt;key&gt; - delete an object",
  "/delbucket &lt;name&gt; - delete a bucket and its objects (confirm with yes)",
  "/help - this list",
  "",
  "Upload: send a file (or photo) with caption &lt;bucket&gt; &lt;key&gt;. Send photos as files to keep the original quality.",
].join("\n");

export interface BotDeps {
  name: string;
  tg: TelegramClient;
  store: Store;
  snapshot: Snapshot;
  ownerIds: Set<number>;
}

interface UploadSource {
  fileId: string;
  size: number;
  contentType: string;
}

function escapeHtml(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function uploadSource(msg: TgMessage): UploadSource | null {
  if (msg.document) {
    return {
      fileId: msg.document.file_id,
      size: msg.document.file_size ?? 0,
      contentType: msg.document.mime_type ?? "application/octet-stream",
    };
  }
  const sizes = msg.photo ?? [];
  if (sizes.length === 0) return null;
  const best = sizes.reduce((a, b) => (b.width * b.height > a.width * a.height ? b : a));
  return { fileId: best.file_id, size: best.file_size ?? 0, contentType: "image/jpeg" };
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  const timer = setTimeout(resolve, ms);
  signal.addEventListener(
    "abort",
    () => {
      clearTimeout(timer);
      resolve();
    },
    { once: true },
  );
  return promise;
}

// Owner-only control bot, driven by long polling. Only private chats from OWNER_IDS are handled.
export class Bot {
  private abort: AbortController | null = null;
  private loop: Promise<void> | null = null;
  private offset = 0;

  constructor(private readonly deps: BotDeps) {}

  start(): void {
    const abort = new AbortController();
    this.abort = abort;
    this.loop = this.poll(abort.signal);
  }

  async stop(): Promise<void> {
    this.abort?.abort();
    await this.loop;
  }

  private async poll(signal: AbortSignal): Promise<void> {
    const { tg } = this.deps;
    while (!signal.aborted) {
      try {
        const updates = await tg.call<TgUpdate[]>(
          "getUpdates",
          { offset: this.offset, timeout: POLL_TIMEOUT_S, allowed_updates: ["message"] },
          { signal, timeoutMs: (POLL_TIMEOUT_S + 15) * 1000 },
        );
        for (const update of updates) {
          this.offset = update.update_id + 1;
          if (update.message) await this.handle(update.message);
        }
      } catch (err) {
        if (signal.aborted) return;
        if (isBotLost(err)) {
          log.error(`${this.deps.name}: stopped polling`, { err: errorText(err) });
          return;
        }
        log.error(`${this.deps.name}: getUpdates failed, retrying`, { err: errorText(err) });
        await sleep(RETRY_DELAY_MS, signal);
      }
    }
  }

  private async handle(msg: TgMessage): Promise<void> {
    const ownerId = msg.from?.id;
    if (msg.chat.type !== "private" || ownerId === undefined || !this.deps.ownerIds.has(ownerId)) return;
    const chatId = msg.chat.id;
    try {
      const source = uploadSource(msg);
      if (source) await this.upload(chatId, msg, source);
      else if (msg.text?.startsWith("/")) await this.command(chatId, msg.text);
    } catch (err) {
      await this.reply(chatId, `Error: ${escapeHtml(errorText(err))}`).catch((replyErr: unknown) =>
        log.error("reply failed", { err: errorText(replyErr) }),
      );
    }
  }

  private reply(chatId: number, html: string): Promise<void> {
    return this.deps.tg
      .call("sendMessage", { chat_id: chatId, text: html, parse_mode: "HTML" })
      .then(() => undefined);
  }

  private async command(chatId: number, text: string): Promise<void> {
    const [raw = "", ...args] = text.trim().split(/\s+/);
    const [name = ""] = raw.split("@");
    const bucket = args[0] ?? "";
    switch (name.toLowerCase()) {
      case "/start":
      case "/help":
        return this.reply(chatId, HELP);
      case "/newbucket":
        return this.newBucket(chatId, bucket);
      case "/rotate":
        return this.rotate(chatId, bucket, args[1] === "now");
      case "/buckets":
        return this.buckets(chatId);
      case "/ls":
        return this.ls(chatId, bucket, args.slice(1).join(" "));
      case "/get":
        return this.get(chatId, bucket, args.slice(1).join(" "));
      case "/rm":
        return this.rm(chatId, bucket, args.slice(1).join(" "));
      case "/delbucket":
        return this.delBucket(chatId, bucket, args[1] === "yes");
      default:
        return this.reply(chatId, "Unknown command. Send /help.");
    }
  }

  private async newBucket(chatId: number, name: string): Promise<void> {
    let token: string;
    try {
      token = this.deps.store.createBucket(name, "owner").token;
    } catch (err) {
      if (err instanceof InvalidNameError) {
        return this.reply(
          chatId,
          "Invalid bucket name. Use 1-63 chars: a-z, 0-9, _ or -, starting with a letter or digit.",
        );
      }
      if (err instanceof BucketExistsError) return this.reply(chatId, "Bucket already exists.");
      throw err;
    }
    await this.deps.snapshot.flush();
    return this.reply(
      chatId,
      `Bucket <b>${escapeHtml(name)}</b> created.\nPassword (shown once):\n<code>${token}</code>`,
    );
  }

  private async rotate(chatId: number, name: string, now: boolean): Promise<void> {
    const issued = this.deps.store.rotateBucket(name, now ? 0 : ROTATE_GRACE_MS);
    if (issued === null) return this.reply(chatId, "No such bucket.");
    await this.deps.snapshot.flush();
    const oldState = now
      ? "Old passwords no longer work."
      : `Old passwords keep working for 24 hours. Send /rotate ${escapeHtml(name)} now to cut them off immediately.`;
    return this.reply(
      chatId,
      `Bucket <b>${escapeHtml(name)}</b> rotated.\nNew password (shown once):\n<code>${issued.token}</code>\n${oldState}`,
    );
  }

  private buckets(chatId: number): Promise<void> {
    const list = this.deps.store.listBuckets();
    if (list.length === 0) return this.reply(chatId, "No buckets yet.");
    const lines = list.map(
      (b) => `${escapeHtml(b.name)} — ${b.objectCount} objects, ${(b.totalBytes / MIB).toFixed(1)} MB`,
    );
    return this.reply(chatId, lines.join("\n"));
  }

  private async ls(chatId: number, bucket: string, prefix: string): Promise<void> {
    const page = this.deps.store.list(bucket, prefix, LS_PAGE, null);
    if (page.objects.length === 0) return this.reply(chatId, "No objects.");
    const lines = page.objects.map((o) => `${escapeHtml(o.key)} (${o.size} bytes)`);
    if (page.nextCursor !== null) lines.push("…and more");
    return this.reply(chatId, lines.join("\n"));
  }

  private async get(chatId: number, bucket: string, key: string): Promise<void> {
    const head = this.deps.store.head(bucket, key);
    if (!head) return this.reply(chatId, "Not found.");
    if (head.size > MAX_BOT_SEND_BYTES) {
      return this.reply(chatId, "Too large to send through Telegram (max 48 MB). Use the HTTP API.");
    }
    const got = await this.deps.store.read(bucket, key, null);
    if (!got) return this.reply(chatId, "Not found.");
    const bytes = got.body instanceof Uint8Array ? got.body : await Bun.readableStreamToBytes(got.body);
    const form = new FormData();
    form.set("chat_id", String(chatId));
    form.set("document", new Blob([bytes]), key.split("/").at(-1) || "file");
    await this.deps.tg.callMultipart("sendDocument", form);
  }

  private async rm(chatId: number, bucket: string, key: string): Promise<void> {
    const deleted = this.deps.store.delete(bucket, key, null);
    return this.reply(chatId, deleted ? "Deleted." : "Not found.");
  }

  private async delBucket(chatId: number, name: string, confirmed: boolean): Promise<void> {
    const info = this.deps.store.listBuckets().find((b) => b.name === name);
    if (!info) return this.reply(chatId, "No such bucket.");
    if (!confirmed) {
      return this.reply(
        chatId,
        `This deletes bucket <b>${escapeHtml(name)}</b> and its ${info.objectCount} objects. Send /delbucket ${escapeHtml(name)} yes to confirm.`,
      );
    }
    const removed = this.deps.store.deleteBucket(name);
    if (removed < 0) return this.reply(chatId, "No such bucket.");
    await this.deps.snapshot.flush();
    return this.reply(chatId, `Deleted bucket ${escapeHtml(name)} (${removed} objects).`);
  }

  private async upload(chatId: number, msg: TgMessage, source: UploadSource): Promise<void> {
    const tokens = (msg.caption ?? "").trim().split(/\s+/);
    if (tokens.length < 2) return this.reply(chatId, "Add a caption: &lt;bucket&gt; &lt;key&gt;");
    const [bucket = "", ...keyParts] = tokens;
    const key = keyParts.join(" ");
    const invalid = keyError(key);
    if (invalid) return this.reply(chatId, escapeHtml(invalid));
    if (source.size > MAX_BOT_UPLOAD_BYTES) return this.reply(chatId, "Too large (max 19 MB).");

    const body = await this.deps.tg.downloadFile(source.fileId);
    try {
      const { meta } = await this.deps.store.put(bucket, key, [body], {
        contentType: source.contentType,
        metadata: {},
        cond: null,
        declaredBytes: body.byteLength,
      });
      return this.reply(
        chatId,
        `Stored <code>${escapeHtml(`${bucket}/${key}`)}</code> (${meta.size} bytes).`,
      );
    } catch (err) {
      if (err instanceof NoSuchBucketError) return this.reply(chatId, "No such bucket.");
      throw err;
    }
  }
}
