import { SerialQueue, TelegramClient, TelegramError, type TgUser } from "./telegram";

export interface BotLane {
  readonly index: number; // 0-based position in TELEGRAM_BOT_TOKENS
  readonly botId: number; // getMe id
  readonly username: string; // getMe username, "" when absent
  readonly tg: TelegramClient;
  readonly queue: SerialQueue; // this bot's channel writes, CHANNEL_MIN_INTERVAL_MS apart
  disabled: boolean;
}

function messageOf(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
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
          throw new Error(`bot #${index + 1}: ${messageOf(err)}`);
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
    console.error(`${laneName(lane)} disabled: ${messageOf(err)}`);
    return true;
  }
}
