import { createApiHandler } from "./api";
import { Bot } from "./bot";
import type { Config } from "./config";
import { importMasterKey } from "./crypto";
import { openDb } from "./db";
import { Snapshot } from "./snapshot";
import { MAX_OBJECT_BYTES, Store } from "./store";
import { SerialQueue, TelegramClient, type TgChatFullInfo, type TgChatMember, type TgUser } from "./telegram";

const REQUEST_OVERHEAD_BYTES = 1024 * 1024;

export interface App {
  port: number;
  stop(): Promise<void>;
}

// Startup checks run before any resource is opened, so a failed check leaves nothing behind.
export async function createApp(config: Config): Promise<App> {
  const tg = new TelegramClient({ apiBase: config.apiBase, token: config.botToken });

  const me = await tg.call<TgUser>("getMe");
  if (config.botPolling) await tg.call("deleteWebhook", { drop_pending_updates: false });

  const chat = await tg.call<TgChatFullInfo>("getChat", { chat_id: config.channelId });
  if (chat.type !== "channel") throw new Error("STORAGE_CHANNEL_ID must be a channel");

  const member = await tg.call<TgChatMember>("getChatMember", { chat_id: config.channelId, user_id: me.id });
  if (
    member.status !== "administrator" ||
    !member.can_post_messages ||
    !member.can_edit_messages ||
    !member.can_delete_messages
  ) {
    throw new Error("bot must be a channel admin with Post, Edit and Delete messages rights");
  }

  const key = await importMasterKey(config.masterKey);
  const db = openDb(config.dbPath);
  const channelQueue = new SerialQueue(config.channelMinIntervalMs);
  const snapshot = new Snapshot({
    db,
    tg,
    channelQueue,
    channelId: config.channelId,
    key,
    intervalMs: config.snapshotIntervalMs,
  });

  try {
    await snapshot.restoreIfEmpty();
  } catch (err) {
    db.close();
    throw err;
  }
  snapshot.start();

  try {
    const store = new Store({
      db,
      tg,
      channelQueue,
      channelId: config.channelId,
      key,
      snapshot,
      queueMax: config.queueMax,
    });
    const server = Bun.serve({
      port: config.port,
      fetch: createApiHandler(store),
      maxRequestBodySize: MAX_OBJECT_BYTES + REQUEST_OVERHEAD_BYTES,
    });
    const bot = new Bot({ tg, store, snapshot, ownerIds: config.ownerIds });
    if (config.botPolling) bot.start();

    if (server.port === undefined) throw new Error("server has no TCP port");
    return {
      port: server.port,
      stop: async () => {
        await bot.stop();
        server.stop(true);
        await snapshot.stop();
        db.close();
      },
    };
  } catch (err) {
    await snapshot.stop();
    db.close();
    throw err;
  }
}
