export interface Config {
  botTokens: string[];
  channelId: string;
  ownerIds: Set<number>;
  masterKey: Uint8Array<ArrayBuffer>;
  port: number;
  dbPath: string;
  apiBase: string;
  snapshotIntervalMs: number;
  channelMinIntervalMs: number;
  queueMaxBytes: number;
  maxObjectBytes: number;
  adminToken: string | null;
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

const BOT_TOKEN_RE = /^\d+:[A-Za-z0-9_-]+$/;

// Comma-separated, one token per bot. A repeated token would be the same bot twice, so it is rejected.
function parseBotTokens(env: Env): string[] {
  const tokens = required(env, "TELEGRAM_BOT_TOKENS")
    .split(",")
    .map((s) => s.trim());
  for (const token of tokens) {
    if (!BOT_TOKEN_RE.test(token)) fail("TELEGRAM_BOT_TOKENS", "must be comma-separated bot tokens");
  }
  if (new Set(tokens).size !== tokens.length) fail("TELEGRAM_BOT_TOKENS", "must not contain the same token twice");
  return tokens;
}

const ADMIN_TOKEN_RE = /^[\x21-\x7e]{32,256}$/;

// Unset means the admin API is off.
function parseAdminToken(env: Env): string | null {
  const v = read(env, "ADMIN_TOKEN");
  if (v === undefined) return null;
  if (!ADMIN_TOKEN_RE.test(v)) fail("ADMIN_TOKEN", "must be 32-256 printable ASCII characters without spaces");
  return v;
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
    botTokens: parseBotTokens(env),
    channelId: required(env, "STORAGE_CHANNEL_ID"),
    ownerIds,
    masterKey,
    port: intVar(env, "PORT", 8080, 0, 65535),
    dbPath: read(env, "DB_PATH") ?? "./data/index.sqlite",
    apiBase: read(env, "TELEGRAM_API_BASE") ?? "https://api.telegram.org",
    snapshotIntervalMs: intVar(env, "SNAPSHOT_INTERVAL_MS", 60_000, 1),
    channelMinIntervalMs: intVar(env, "CHANNEL_MIN_INTERVAL_MS", 3_000, 0),
    queueMaxBytes: intVar(env, "QUEUE_MAX_BYTES", 67_108_864, 1),
    maxObjectBytes: intVar(env, "MAX_OBJECT_BYTES", 5_368_709_120, 1),
    adminToken: parseAdminToken(env),
    botPolling: read(env, "BOT_POLLING") !== "false",
  };
}
