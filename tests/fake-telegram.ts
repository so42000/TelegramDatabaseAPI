// In-memory stand-in for the Telegram Bot API. Implements only the methods the app calls,
// with the same envelope, multipart and attach:// conventions as the real API.
// Each bot has its own token, inbox and file_ids, as on real Telegram.

export const FAKE_CHANNEL_ID = "-1001234567890";
export const FAKE_BOT_ID = 123456;
const CHANNEL_ID = Number(FAKE_CHANNEL_ID);

type Params = Record<string, unknown>;

export interface FakeDocument {
  file_id: string;
  file_unique_id: string;
  file_name?: string;
  mime_type?: string;
  file_size: number;
}

export interface FakeMessage {
  message_id: number;
  date: number;
  chat: { id: number; type: string };
  caption?: string;
  document?: FakeDocument;
}

export interface SentMessage {
  chatId: string;
  text: string;
}

export interface FakeState {
  messages: Map<number, FakeMessage>;
  files: Map<string, Uint8Array<ArrayBuffer>>;
  pinned: number | null;
  calls: string[];
  botCalls: { botId: number; method: string }[];
  deleted: number[];
  sent: SentMessage[];
  fail429: Set<string>;
  retryAfterS: number;
  // file_id -> bot that can use it. Files with no entry are fixtures every bot can read.
  fileOwners: Map<string, number>;
  // Bots whose token is revoked: every call and file download returns 401.
  lostBots: Set<number>;
  // Queues an incoming message for one bot. The promise resolves once that bot has finished handling it,
  // which is observable because the bot only calls getUpdates again after its batch is handled.
  sendUpdate: (message: Record<string, unknown>, botId?: number) => Promise<void>;
}

export interface FakeTelegram {
  apiBase: string;
  state: FakeState;
  stop(): void;
}

class ApiError extends Error {
  constructor(
    readonly code: number,
    message: string,
  ) {
    super(message);
  }
}

interface Update {
  update_id: number;
  message: Record<string, unknown>;
}

// Incoming updates waiting for one bot's getUpdates, the resolvers of the batch that bot is handling,
// and long-poll waiters parked until an update arrives.
interface Inbox {
  queued: { update: Update; done: () => void }[];
  handling: (() => void)[];
  wakers: (() => void)[];
}

function botIdOf(token: string): number {
  return Number(token.split(":")[0]);
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

async function readParams(req: Request): Promise<Params> {
  if ((req.headers.get("content-type") ?? "").startsWith("multipart/form-data")) {
    const params: Params = {};
    for (const [k, v] of await req.formData()) params[k] = v;
    return params;
  }
  const text = await req.text();
  return text === "" ? {} : (JSON.parse(text) as Params);
}

export function startFakeTelegram(): FakeTelegram {
  let seq = 1000;
  const nextSeq = () => seq++;
  const now = () => Math.floor(Date.now() / 1000);

  const inboxes = new Map<number, Inbox>();
  const inboxOf = (botId: number): Inbox => {
    let box = inboxes.get(botId);
    if (!box) {
      box = { queued: [], handling: [], wakers: [] };
      inboxes.set(botId, box);
    }
    return box;
  };

  const state: FakeState = {
    messages: new Map(),
    files: new Map(),
    pinned: null,
    calls: [],
    botCalls: [],
    deleted: [],
    sent: [],
    fail429: new Set(),
    retryAfterS: 1,
    fileOwners: new Map(),
    lostBots: new Set(),
    sendUpdate: (message, botId = FAKE_BOT_ID) => {
      const box = inboxOf(botId);
      const { promise, resolve } = Promise.withResolvers<void>();
      box.queued.push({
        update: { update_id: nextSeq(), message: { message_id: nextSeq(), date: now(), ...message } },
        done: resolve,
      });
      for (const wake of box.wakers.splice(0)) wake();
      return promise;
    },
  };

  const chatRef = (chatId: string) => ({
    id: Number(chatId),
    type: Number(chatId) === CHANNEL_ID ? "channel" : "private",
  });

  // Each bot's file_id for the same bytes is a different string, as on real Telegram.
  function fileIdFor(from: string, botId: number): string {
    const bytes = state.files.get(from);
    if (!bytes) throw new ApiError(400, "Bad Request: wrong file_id or the file is temporarily unavailable");
    const fileId = `file-${nextSeq()}`;
    state.files.set(fileId, bytes);
    state.fileOwners.set(fileId, botId);
    return fileId;
  }

  // What getChat shows a bot: the pinned message with a file_id that bot can use.
  function viewFor(message: FakeMessage | undefined, botId: number): FakeMessage | undefined {
    if (!message?.document) return message;
    const owner = state.fileOwners.get(message.document.file_id);
    if (owner === undefined || owner === botId) return message;
    return { ...message, document: { ...message.document, file_id: fileIdFor(message.document.file_id, botId) } };
  }

  async function storeFile(blob: Blob, botId: number): Promise<FakeDocument> {
    const fileId = `file-${nextSeq()}`;
    const bytes = new Uint8Array(await blob.arrayBuffer());
    state.files.set(fileId, bytes);
    state.fileOwners.set(fileId, botId);
    return {
      file_id: fileId,
      file_unique_id: fileId,
      file_name: blob instanceof File ? blob.name : undefined,
      mime_type: blob.type || undefined,
      file_size: bytes.byteLength,
    };
  }

  async function addDocument(
    chatId: string,
    blob: Blob,
    caption: string | undefined,
    botId: number,
  ): Promise<FakeMessage> {
    const message: FakeMessage = {
      message_id: nextSeq(),
      date: now(),
      chat: chatRef(chatId),
      caption,
      document: await storeFile(blob, botId),
    };
    state.messages.set(message.message_id, message);
    return message;
  }

  async function dispatch(method: string, p: Params, botId: number): Promise<unknown> {
    switch (method) {
      case "getMe":
        return { id: botId, is_bot: true, first_name: "tgstore-test", username: `tgstore_test_${botId}_bot` };
      case "deleteWebhook":
        return true;
      case "getChat":
        return {
          id: CHANNEL_ID,
          type: "channel",
          pinned_message: viewFor(state.pinned === null ? undefined : state.messages.get(state.pinned), botId),
        };
      case "getChatMember":
        return { status: "administrator", can_post_messages: true, can_edit_messages: true, can_delete_messages: true };
      case "sendDocument":
        return addDocument(
          String(p.chat_id),
          p.document as Blob,
          typeof p.caption === "string" ? p.caption : undefined,
          botId,
        );
      case "editMessageMedia": {
        const message = state.messages.get(Number(p.message_id));
        if (!message) throw new ApiError(400, "Bad Request: message to edit not found");
        const media = JSON.parse(String(p.media)) as { media: string; caption?: string };
        const blob = p[media.media.replace(/^attach:\/\//, "")];
        if (!(blob instanceof Blob)) throw new ApiError(400, "Bad Request: media attachment missing");
        message.document = await storeFile(blob, botId);
        message.caption = media.caption;
        return message;
      }
      case "editMessageCaption": {
        const message = state.messages.get(Number(p.message_id));
        if (!message) throw new ApiError(400, "Bad Request: message to edit not found");
        const caption = String(p.caption);
        if (caption === message.caption) throw new ApiError(400, "Bad Request: message is not modified");
        message.caption = caption;
        if (!message.document) return message;
        // The stored message keeps its file_id; the editing bot gets its own.
        const fileId = fileIdFor(message.document.file_id, botId);
        return { ...message, document: { ...message.document, file_id: fileId } };
      }
      case "pinChatMessage": {
        const id = Number(p.message_id);
        if (!state.messages.has(id)) throw new ApiError(400, "Bad Request: message not found");
        state.pinned = id;
        return true;
      }
      case "deleteMessage": {
        const id = Number(p.message_id);
        state.deleted.push(id);
        if (!state.messages.delete(id)) throw new ApiError(400, "Bad Request: message to delete not found");
        return true;
      }
      case "sendMessage": {
        state.sent.push({ chatId: String(p.chat_id), text: String(p.text) });
        return {
          message_id: nextSeq(),
          date: now(),
          chat: chatRef(String(p.chat_id)),
          text: String(p.text),
        };
      }
      case "getFile": {
        const fileId = String(p.file_id);
        const bytes = state.files.get(fileId);
        const owner = state.fileOwners.get(fileId);
        if (!bytes || (owner !== undefined && owner !== botId)) {
          throw new ApiError(400, "Bad Request: wrong file_id or the file is temporarily unavailable");
        }
        return { file_id: fileId, file_unique_id: fileId, file_size: bytes.byteLength, file_path: fileId };
      }
      case "getUpdates": {
        const box = inboxOf(botId);
        // Previous batch is fully handled once the bot asks again.
        for (const done of box.handling) done();
        box.handling = [];
        while (box.queued.length === 0) await new Promise<void>((wake) => box.wakers.push(wake));
        const batch = box.queued.splice(0);
        box.handling = batch.map((b) => b.done);
        return batch.map((b) => b.update);
      }
      default:
        throw new ApiError(404, `Not Found: unsupported method ${method}`);
    }
  }

  async function handle(req: Request): Promise<Response> {
    const path = new URL(req.url).pathname;
    const fileMatch = /^\/file\/bot([^/]+)\/(.+)$/.exec(path);
    if (fileMatch && req.method === "GET") {
      if (state.lostBots.has(botIdOf(fileMatch[1] ?? ""))) return new Response("unauthorized", { status: 401 });
      const bytes = state.files.get(decodeURIComponent(fileMatch[2] ?? ""));
      if (!bytes) return new Response("not found", { status: 404 });
      const range = /^bytes=(\d+)-(\d+)$/.exec(req.headers.get("range") ?? "");
      if (!range) return new Response(bytes);
      const start = Number(range[1]);
      const end = Number(range[2]);
      return new Response(bytes.slice(start, end + 1), {
        status: 206,
        headers: { "content-range": `bytes ${start}-${end}/${bytes.byteLength}` },
      });
    }

    const callMatch = /^\/bot([^/]+)\/([A-Za-z]+)$/.exec(path);
    if (!callMatch || req.method !== "POST") return new Response("not found", { status: 404 });
    const botId = botIdOf(callMatch[1] ?? "");
    const method = callMatch[2] ?? "";
    const params = await readParams(req);
    state.calls.push(method);
    state.botCalls.push({ botId, method });

    if (state.lostBots.has(botId)) {
      return json({ ok: false, error_code: 401, description: "Unauthorized" }, 401);
    }
    if (state.fail429.delete(method)) {
      return json(
        { ok: false, error_code: 429, description: "Too Many Requests: retry later", parameters: { retry_after: state.retryAfterS } },
        429,
      );
    }
    try {
      return json({ ok: true, result: await dispatch(method, params, botId) });
    } catch (err) {
      if (err instanceof ApiError) return json({ ok: false, error_code: err.code, description: err.message }, err.code);
      throw err;
    }
  }

  const server = Bun.serve({ port: 0, fetch: handle });
  return {
    apiBase: `http://localhost:${server.port}`,
    state,
    stop: () => {
      server.stop(true);
    },
  };
}
