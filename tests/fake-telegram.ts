// In-memory stand-in for the Telegram Bot API. Implements only the methods the app calls,
// with the same envelope, multipart and attach:// conventions as the real API.

export const FAKE_CHANNEL_ID = "-1001234567890";
export const FAKE_BOT_ID = 999;
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
  deleted: number[];
  sent: SentMessage[];
  fail429: Set<string>;
  // Queues an incoming message. The promise resolves once the bot has finished handling it,
  // which is observable because the bot only calls getUpdates again after its batch is handled.
  sendUpdate: (message: Record<string, unknown>) => Promise<void>;
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

  // Incoming updates waiting for getUpdates, the resolvers of the batch the bot is handling,
  // and long-poll waiters parked until an update arrives.
  const queued: { update: { update_id: number; message: Record<string, unknown> }; done: () => void }[] = [];
  let handling: (() => void)[] = [];
  const wakers: (() => void)[] = [];

  const state: FakeState = {
    messages: new Map(),
    files: new Map(),
    pinned: null,
    calls: [],
    deleted: [],
    sent: [],
    fail429: new Set(),
    sendUpdate: (message) => {
      const { promise, resolve } = Promise.withResolvers<void>();
      queued.push({
        update: { update_id: nextSeq(), message: { message_id: nextSeq(), date: now(), ...message } },
        done: resolve,
      });
      for (const wake of wakers.splice(0)) wake();
      return promise;
    },
  };

  const chatRef = (chatId: string) => ({
    id: Number(chatId),
    type: Number(chatId) === CHANNEL_ID ? "channel" : "private",
  });

  async function storeFile(blob: Blob): Promise<FakeDocument> {
    const fileId = `file-${nextSeq()}`;
    const bytes = new Uint8Array(await blob.arrayBuffer());
    state.files.set(fileId, bytes);
    return {
      file_id: fileId,
      file_unique_id: fileId,
      file_name: blob instanceof File ? blob.name : undefined,
      mime_type: blob.type || undefined,
      file_size: bytes.byteLength,
    };
  }

  async function addDocument(chatId: string, blob: Blob, caption: string | undefined): Promise<FakeMessage> {
    const message: FakeMessage = {
      message_id: nextSeq(),
      date: now(),
      chat: chatRef(chatId),
      caption,
      document: await storeFile(blob),
    };
    state.messages.set(message.message_id, message);
    return message;
  }

  async function dispatch(method: string, p: Params): Promise<unknown> {
    switch (method) {
      case "getMe":
        return { id: FAKE_BOT_ID, is_bot: true, first_name: "tgstore-test", username: "tgstore_test_bot" };
      case "deleteWebhook":
        return true;
      case "getChat":
        return {
          id: CHANNEL_ID,
          type: "channel",
          pinned_message: state.pinned === null ? undefined : state.messages.get(state.pinned),
        };
      case "getChatMember":
        return { status: "administrator", can_post_messages: true, can_edit_messages: true, can_delete_messages: true };
      case "sendDocument":
        return addDocument(
          String(p.chat_id),
          p.document as Blob,
          typeof p.caption === "string" ? p.caption : undefined,
        );
      case "editMessageMedia": {
        const message = state.messages.get(Number(p.message_id));
        if (!message) throw new ApiError(400, "Bad Request: message to edit not found");
        const media = JSON.parse(String(p.media)) as { media: string; caption?: string };
        const blob = p[media.media.replace(/^attach:\/\//, "")];
        if (!(blob instanceof Blob)) throw new ApiError(400, "Bad Request: media attachment missing");
        message.document = await storeFile(blob);
        message.caption = media.caption;
        return message;
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
        if (!bytes) throw new ApiError(400, "Bad Request: file not found");
        return { file_id: fileId, file_unique_id: fileId, file_size: bytes.byteLength, file_path: fileId };
      }
      case "getUpdates": {
        // Previous batch is fully handled once the bot asks again.
        for (const done of handling) done();
        handling = [];
        while (queued.length === 0) await new Promise<void>((wake) => wakers.push(wake));
        const batch = queued.splice(0);
        handling = batch.map((b) => b.done);
        return batch.map((b) => b.update);
      }
      default:
        throw new ApiError(404, `Not Found: unsupported method ${method}`);
    }
  }

  async function handle(req: Request): Promise<Response> {
    const path = new URL(req.url).pathname;
    const fileMatch = /^\/file\/bot[^/]+\/(.+)$/.exec(path);
    if (fileMatch && req.method === "GET") {
      const bytes = state.files.get(decodeURIComponent(fileMatch[1] ?? ""));
      return bytes ? new Response(bytes) : new Response("not found", { status: 404 });
    }

    const callMatch = /^\/bot[^/]+\/([A-Za-z]+)$/.exec(path);
    if (!callMatch || req.method !== "POST") return new Response("not found", { status: 404 });
    const method = callMatch[1] ?? "";
    const params = await readParams(req);
    state.calls.push(method);

    if (state.fail429.delete(method)) {
      return json(
        { ok: false, error_code: 429, description: "Too Many Requests: retry later", parameters: { retry_after: 1 } },
        429,
      );
    }
    try {
      return json({ ok: true, result: await dispatch(method, params) });
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
