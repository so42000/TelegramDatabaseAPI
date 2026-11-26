import { errorText, log } from "./log";
import { SerialQueue, TelegramClient, TelegramError, type TgMessage, type TgUser } from "./telegram";
import { botsDisabled } from "./metrics";

export interface BotLane {
  readonly index: number; // 0-based position in TELEGRAM_BOT_TOKENS
  readonly botId: number; // getMe id
  readonly username: string; // getMe username, "" when absent
  readonly tg: TelegramClient;
  readonly queue: SerialQueue; // this bot's channel writes, CHANNEL_MIN_INTERVAL_MS apart
  disabled: boolean;
}

export function laneName(lane: BotLane): string {
  return lane.username === "" ? `bot #${lane.index + 1} (${lane.botId})` : `bot #${lane.index + 1} (@${lane.username})`;
}

// 401: token revoked or bot deleted/banned. 403: bot kicked from the channel or lost a right.
export function isBotLost(err: unknown): err is TelegramError {
  return err instanceof TelegramError && (err.code === 401 || err.code === 403);
}

export class BotPool {
  constructor(readonly lanes: BotLane[]) {}

  static async connect(opts: { apiBase: string; tokens: string[]; minIntervalMs: number }): Promise<BotPool> {
    const lanes = await Promise.all(
      opts.tokens.map(async (token, index): Promise<BotLane> => {
        const tg = new TelegramClient({ apiBase: opts.apiBase, token });
        try {
          const me = await tg.call<TgUser>("getMe");
          return {
            index,
            botId: me.id,
            username: me.username ?? "",
            tg,
            queue: new SerialQueue(opts.minIntervalMs),
            disabled: false,
          };
        } catch (err) {
          throw new Error(`bot #${index + 1}: ${errorText(err)}`);
        }
      }),
    );
    const seen = new Map<number, number>();
    for (const lane of lanes) {
      const first = seen.get(lane.botId);
      if (first !== undefined) throw new Error(`bot #${first + 1} and bot #${lane.index + 1} are the same bot`);
      seen.set(lane.botId, lane.index);
    }
    return new BotPool(lanes);
  }

  // Runs snapshot restore, flush, pin and pending deletes.
  primary(): BotLane {
    const lane = this.lanes.find((l) => !l.disabled);
    if (!lane) throw new Error("no enabled bot");
    return lane;
  }

  byBotId(botId: number): BotLane | undefined {
    return this.lanes.find((l) => !l.disabled && l.botId === botId);
  }

  // Enabled lanes and how many of them answered their last request.
  health(): { total: number; enabled: number; reachable: number } {
    const enabled = this.lanes.filter((lane) => !lane.disabled);
    return {
      total: this.lanes.length,
      enabled: enabled.length,
      reachable: enabled.filter((lane) => lane.tg.reachable).length,
    };
  }

  // Lane whose next write can start soonest. Ties go to the lower index.
  pickWriter(): BotLane {
    let best: BotLane | undefined;
    let bestAt = Infinity;
    for (const lane of this.lanes) {
      if (lane.disabled) continue;
      const at = lane.queue.nextStartAt();
      if (best === undefined || at < bestAt) {
        best = lane;
        bestAt = at;
      }
    }
    if (!best) throw new Error("no enabled bot");
    return best;
  }

  // Returns false only when lane is the last enabled one, which stays enabled.
  disable(lane: BotLane, err: unknown): boolean {
    if (lane.disabled) return true;
    if (this.lanes.filter((l) => !l.disabled).length === 1) return false;
    lane.disabled = true;
    botsDisabled.inc({});
    log.error(`${laneName(lane)} disabled`, { err: errorText(err) });
    return true;
  }

  // File ids work only for the bot that received them. Editing a batch message's caption (allowed for any
  // channel admin with Edit rights) returns the message with a file_id for the editing bot, which then owns
  // every part in it. Captions are random UUIDs, so the edit reveals nothing.
  async takeOver(channelId: string, messageId: number): Promise<{ lane: BotLane; fileId: string }> {
    for (;;) {
      const lane = this.pickWriter();
      let msg: TgMessage;
      try {
        msg = await lane.queue.run(() =>
          lane.tg.call<TgMessage>("editMessageCaption", {
            chat_id: channelId,
            message_id: messageId,
            caption: crypto.randomUUID(),
          }),
        );
      } catch (err) {
        if (isBotLost(err) && this.disable(lane, err)) continue;
        throw err;
      }
      if (!msg.document) throw new Error(`message ${messageId} has no document`);
      return { lane, fileId: msg.document.file_id };
    }
  }
}
