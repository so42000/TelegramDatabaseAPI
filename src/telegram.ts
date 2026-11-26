import { telegramRequests } from "./metrics";

export interface TgUser {
  id: number;
  username?: string;
}

export interface TgDocument {
  file_id: string;
  file_name?: string;
  mime_type?: string;
  file_size?: number;
}

export interface TgPhotoSize {
  file_id: string;
  width: number;
  height: number;
  file_size?: number;
}

export interface TgMessage {
  message_id: number;
  chat: { id: number; type: string };
  from?: TgUser;
  text?: string;
  caption?: string;
  document?: TgDocument;
  photo?: TgPhotoSize[];
}

export interface TgUpdate {
  update_id: number;
  message?: TgMessage;
}

export interface TgFile {
  file_id: string;
  file_path?: string;
  file_size?: number;
}

export interface TgChatFullInfo {
  id: number;
  type: string;
  pinned_message?: TgMessage;
}

export interface TgChatMember {
  status: string;
  can_post_messages?: boolean;
  can_edit_messages?: boolean;
  can_delete_messages?: boolean;
}

export class TelegramError extends Error {
  constructor(
    readonly method: string,
    readonly code: number,
    readonly description: string,
  ) {
    super(`telegram ${method} failed: ${code} ${description}`);
    this.name = "TelegramError";
  }
}

interface Envelope<T> {
  ok: boolean;
  result?: T;
  error_code?: number;
  description?: string;
  parameters?: { retry_after?: number };
}

export interface ByteRange {
  offset: number;
  length: number;
}

export interface CallOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}

const MAX_RATE_LIMIT_ATTEMPTS = 5;
const MAX_TRANSIENT_ATTEMPTS = 3;
const FILE_CACHE_TTL_MS = 50 * 60_000;
const FILE_CACHE_MAX = 10_000;
const CALL_TIMEOUT_MS = 30_000;
const UPLOAD_TIMEOUT_MS = 180_000;
const DOWNLOAD_TIMEOUT_MS = 180_000;

function sleep(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, ms);
  return promise;
}

// Metric outcome for a failed HTTP status.
function failureOutcome(status: number): string {
  if (status === 429) return "rate_limited";
  return status >= 500 ? "server_error" : "client_error";
}

export class TelegramClient {
  private readonly apiBase: string;
  private readonly token: string;
  private readonly fileCache = new Map<string, { path: string; expiresAt: number }>();
  // False after a network failure or 5xx that exhausted its retries; read by /readyz.
  reachable = true;

  constructor(opts: { apiBase: string; token: string }) {
    this.apiBase = opts.apiBase;
    this.token = opts.token;
  }

  call<T>(method: string, params: Record<string, unknown> = {}, opts: CallOptions = {}): Promise<T> {
    return this.request<T>(
      method,
      { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(params) },
      { signal: opts.signal, timeoutMs: opts.timeoutMs ?? CALL_TIMEOUT_MS },
    );
  }

  callMultipart<T>(method: string, form: FormData, opts: CallOptions = {}): Promise<T> {
    return this.request<T>(
      method,
      { method: "POST", body: form },
      { signal: opts.signal, timeoutMs: opts.timeoutMs ?? UPLOAD_TIMEOUT_MS },
    );
  }

  // With `range`, returns exactly those bytes. If the file server ignores Range and returns the
  // whole file, the range is sliced locally.
  async downloadFile(fileId: string, range?: ByteRange): Promise<Uint8Array<ArrayBuffer>> {
    const cached = this.fileCache.get(fileId);
    if (cached && cached.expiresAt > Date.now()) {
      const first = await this.fetchFile(cached.path, range);
      if (first.ok) return first.bytes;
      this.fileCache.delete(fileId);
    }
    const path = await this.resolveFilePath(fileId);
    const second = await this.fetchFile(path, range);
    if (second.ok) return second.bytes;
    throw new TelegramError("download", second.status, "file download failed");
  }

  private async resolveFilePath(fileId: string): Promise<string> {
    const file = await this.call<TgFile>("getFile", { file_id: fileId });
    if (!file.file_path) throw new TelegramError("getFile", 0, "no file_path returned");
    this.fileCache.delete(fileId);
    if (this.fileCache.size >= FILE_CACHE_MAX) {
      const oldest = this.fileCache.keys().next().value;
      if (oldest !== undefined) this.fileCache.delete(oldest);
    }
    this.fileCache.set(fileId, { path: file.file_path, expiresAt: Date.now() + FILE_CACHE_TTL_MS });
    return file.file_path;
  }

  private async fetchFile(
    path: string,
    range?: ByteRange,
  ): Promise<{ ok: true; bytes: Uint8Array<ArrayBuffer> } | { ok: false; status: number }> {
    try {
      const headers = range ? { Range: `bytes=${range.offset}-${range.offset + range.length - 1}` } : undefined;
      const res = await fetch(`${this.apiBase}/file/bot${this.token}/${path}`, {
        headers,
        signal: AbortSignal.timeout(DOWNLOAD_TIMEOUT_MS),
      });
      this.reachable = true;
      telegramRequests.inc({ method: "download", outcome: res.ok ? "ok" : failureOutcome(res.status) });
      if (!res.ok) return { ok: false, status: res.status };
      const bytes = new Uint8Array(await res.arrayBuffer());
      if (!range) return { ok: true, bytes };
      const part = res.status === 206 ? bytes : bytes.slice(range.offset, range.offset + range.length);
      if (part.byteLength !== range.length) return { ok: false, status: res.status };
      return { ok: true, bytes: part };
    } catch {
      telegramRequests.inc({ method: "download", outcome: "network" });
      this.reachable = false;
      return { ok: false, status: 0 };
    }
  }

  private async request<T>(
    method: string,
    init: RequestInit,
    opts: { signal?: AbortSignal; timeoutMs: number },
  ): Promise<T> {
    const url = `${this.apiBase}/bot${this.token}/${method}`;
    let rateLimitedAttempts = 0;
    let transientAttempts = 0;
    for (;;) {
      let status: number;
      let text: string;
      try {
        const timeout = AbortSignal.timeout(opts.timeoutMs);
        const res = await fetch(url, { ...init, signal: opts.signal ? AbortSignal.any([opts.signal, timeout]) : timeout });
        status = res.status;
        text = await res.text();
      } catch (err) {
        // An external stop surfaces as AbortError and must not be retried; a per-attempt timeout is.
        if ((err as { name?: unknown } | null)?.name === "AbortError") throw err;
        telegramRequests.inc({ method, outcome: "network" });
        if (transientAttempts >= MAX_TRANSIENT_ATTEMPTS - 1) {
          this.reachable = false;
          throw new TelegramError(method, 0, "network error");
        }
        await sleep(1000 * 2 ** transientAttempts);
        transientAttempts++;
        continue;
      }
      this.reachable = true;

      let env: Envelope<T> | null;
      try {
        env = JSON.parse(text) as Envelope<T>;
      } catch {
        env = null;
      }
      if (env?.ok === true) {
        telegramRequests.inc({ method, outcome: "ok" });
        return env.result as T;
      }
      telegramRequests.inc({ method, outcome: failureOutcome(status) });

      if (env?.error_code === 429 && rateLimitedAttempts < MAX_RATE_LIMIT_ATTEMPTS - 1) {
        rateLimitedAttempts++;
        await sleep((env.parameters?.retry_after ?? 5) * 1000);
        continue;
      }
      if (status >= 500 && transientAttempts < MAX_TRANSIENT_ATTEMPTS - 1) {
        await sleep(1000 * 2 ** transientAttempts);
        transientAttempts++;
        continue;
      }
      if (status >= 500) this.reachable = false;
      throw new TelegramError(method, env?.error_code ?? status, env?.description ?? `http ${status}`);
    }
  }
}

export class SerialQueue {
  private tail: Promise<void> = Promise.resolve();
  private lastStart = -Infinity;
  // Jobs submitted but not yet started.
  private waiting = 0;

  constructor(private readonly minIntervalMs: number) {}

  run<T>(fn: () => Promise<T>): Promise<T> {
    this.waiting++;
    const job = this.tail.then(async () => {
      const wait = this.lastStart + this.minIntervalMs - Date.now();
      if (wait > 0) await sleep(wait);
      this.lastStart = Date.now();
      this.waiting--;
      return fn();
    });
    this.tail = job.then(
      () => undefined,
      () => undefined,
    );
    return job;
  }

  // When a job submitted now would start, ignoring how long a running job takes. Infinity while another job is still waiting.
  nextStartAt(): number {
    return this.waiting > 0 ? Infinity : this.lastStart + this.minIntervalMs;
  }
}
