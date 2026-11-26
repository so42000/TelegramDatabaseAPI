import { createApiHandler } from "./api";
import { Batcher } from "./batcher";
import { Bot } from "./bot";
import type { Config } from "./config";
import { importMasterKey } from "./crypto";
import { openDb } from "./db";
import { Snapshot } from "./snapshot";
import { MAX_OBJECT_BYTES, Store } from "./store";
import { BotPool, laneName } from "./pool";
import type { TgChatFullInfo, TgChatMember } from "./telegram";

const REQUEST_OVERHEAD_BYTES = 1024 * 1024;

export interface App {
  port: number;
  stop(): Promise<void>;
}

// Startup checks run before any resource is opened, so a failed check leaves nothing behind.
export async function createApp(config: Config): Promise<App> {
  const pool = await BotPool.connect({
    apiBase: config.apiBase,
    tokens: config.botTokens,
    minIntervalMs: config.channelMinIntervalMs,
  });

  const chat = await pool.primary().tg.call<TgChatFullInfo>("getChat", { chat_id: config.channelId });
  if (chat.type !== "channel") throw new Error("STORAGE_CHANNEL_ID must be a channel");

  await Promise.all(
    pool.lanes.map(async (lane) => {
      try {
        if (config.botPolling) await lane.tg.call("deleteWebhook", { drop_pending_updates: false });
        const member = await lane.tg.call<TgChatMember>("getChatMember", {
          chat_id: config.channelId,
          user_id: lane.botId,
        });
        if (
          member.status !== "administrator" ||
          !member.can_post_messages ||
          !member.can_edit_messages ||
          !member.can_delete_messages
        ) {
          throw new Error("bot must be a channel admin with Post, Edit and Delete messages rights");
        }
      } catch (err) {
        throw new Error(`${laneName(lane)}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }),
  );

  const key = await importMasterKey(config.masterKey);
  const db = openDb(config.dbPath);
  const snapshot = new Snapshot({ db, pool, channelId: config.channelId, key, intervalMs: config.snapshotIntervalMs });

  try {
    await snapshot.restoreIfEmpty();
  } catch (err) {
    db.close();
    throw err;
  }
  snapshot.start();

  try {
    const batcher = new Batcher({
      db,
      pool,
      channelId: config.channelId,
      snapshot,
      maxPendingBytes: config.queueMaxBytes,
    });
    const store = new Store({ db, pool, batcher, key, snapshot, channelId: config.channelId });
    await store.claimUnownedObjects();
    const server = Bun.serve({
      port: config.port,
      fetch: createApiHandler(store),
      maxRequestBodySize: MAX_OBJECT_BYTES + REQUEST_OVERHEAD_BYTES,
    });
    const bots = pool.lanes.map(
      (lane) => new Bot({ tg: lane.tg, name: laneName(lane), store, snapshot, ownerIds: config.ownerIds }),
    );
    if (config.botPolling) for (const bot of bots) bot.start();

    if (server.port === undefined) throw new Error("server has no TCP port");
    return {
      port: server.port,
      stop: async () => {
        await Promise.all(bots.map((bot) => bot.stop()));
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
