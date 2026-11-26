export interface Config {
  botToken: string;
  channelId: string;
  ownerIds: Set<number>;
  masterKey: Uint8Array<ArrayBuffer>;
  port: number;
  dbPath: string;
  apiBase: string;
  snapshotIntervalMs: number;
  channelMinIntervalMs: number;
  queueMaxBytes: number;
  botPolling: boolean;
}

type Env = Record<string, string | undefined>;

function fail(name: string, problem: string): never {
  throw new Error(`config: ${name} ${problem}`);
}

function read(env: Env, name: string): string | undefined {
  const v = env[name]?.trim();
  return v === undefined || v === "" ? undefined : v;
}

function required(env: Env, name: string): string {
  const v = read(env, name);
  if (v === undefined) fail(name, "is required");
  return v;
}

function intVar(env: Env, name: string, def: number, min: number, max = Number.MAX_SAFE_INTEGER): number {
  const v = read(env, name);
  if (v === undefined) return def;
  if (!/^\d+$/.test(v)) fail(name, "must be a non-negative integer");
  const n = Number(v);
  if (!Number.isSafeInteger(n) || n < min || n > max) fail(name, `must be between ${min} and ${max}`);
  return n;
}

export function loadConfig(env: Env = process.env): Config {
  const ownerIds = new Set<number>();
  for (const part of required(env, "OWNER_IDS").split(",")) {
    const s = part.trim();
    if (!/^\d+$/.test(s)) fail("OWNER_IDS", "must be comma-separated integers");
    ownerIds.add(Number(s));
  }

  const b64 = required(env, "MASTER_KEY");
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(b64)) fail("MASTER_KEY", "must be base64");
  const masterKey = new Uint8Array(Buffer.from(b64, "base64"));
  if (masterKey.byteLength !== 32) fail("MASTER_KEY", "must decode to exactly 32 bytes");

  return {
    botToken: required(env, "TELEGRAM_BOT_TOKEN"),
    channelId: required(env, "STORAGE_CHANNEL_ID"),
    ownerIds,
    masterKey,
    port: intVar(env, "PORT", 8080, 0, 65535),
    dbPath: read(env, "DB_PATH") ?? "./data/index.sqlite",
    apiBase: read(env, "TELEGRAM_API_BASE") ?? "https://api.telegram.org",
    snapshotIntervalMs: intVar(env, "SNAPSHOT_INTERVAL_MS", 60_000, 1),
    channelMinIntervalMs: intVar(env, "CHANNEL_MIN_INTERVAL_MS", 3_000, 0),
    queueMaxBytes: intVar(env, "QUEUE_MAX_BYTES", 67_108_864, 1),
    botPolling: read(env, "BOT_POLLING") !== "false",
  };
}
