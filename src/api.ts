import type { Server } from "bun";
import { keyError, type ByteSpan, type ObjectMeta, type Store } from "./store";
import type { IndexDb, Precondition } from "./db";
import type { Batcher } from "./batcher";
import type { BotPool } from "./pool";
import { handleAdmin, type AdminDeps } from "./admin";
import { log } from "./log";
import { errorResponse, json, methodNotAllowed, notFound, type RequestCtx } from "./http";
import { httpDuration, httpRequests, renderMetrics, type Gauge } from "./metrics";

const OBJECT_PATH = /^\/v1\/buckets\/([^/]+)\/objects(?:\/(.+))?$/;

export interface ApiHandler {
  fetch(req: Request, server: Server<undefined>): Promise<Response>;
  // Resolves once no request is inside fetch(). Response bodies that are still streaming are not waited for.
  drain(): Promise<void>;
}

export interface ApiDeps extends AdminDeps {
  db: IndexDb;
  pool: BotPool;
  batcher: Batcher;
}

const METHODS: Record<string, true> = { GET: true, HEAD: true, PUT: true, DELETE: true, POST: true };
// Probes and scrapes would drown the request log.
const UNLOGGED_ROUTES: Record<string, true> = { healthz: true, readyz: true, metrics: true };
const REQUEST_ID = /^[A-Za-z0-9._-]{1,128}$/;

export function createApiHandler(deps: ApiDeps): ApiHandler {
  let inFlight = 0;
  const idleWaiters: (() => void)[] = [];
  return {
    async fetch(req, server) {
      inFlight++;
      const started = performance.now();
      const reqId = requestId(req.headers.get("x-request-id"));
      const ctx: RequestCtx = { route: "not_found", bucket: null };
      try {
        const res = await route(deps, req, server, ctx).catch((err: unknown) => errorResponse(deps.store, err));
        res.headers.set("X-Request-Id", reqId);
        observe(req, res, ctx, reqId, performance.now() - started);
        return res;
      } finally {
        inFlight--;
        if (inFlight === 0) for (const wake of idleWaiters.splice(0)) wake();
      }
    },
    drain() {
      if (inFlight === 0) return Promise.resolve();
      const { promise, resolve } = Promise.withResolvers<void>();
      idleWaiters.push(resolve);
      return promise;
    },
  };
}

function requestId(incoming: string | null): string {
  return incoming !== null && REQUEST_ID.test(incoming) ? incoming : crypto.randomUUID();
}

function observe(req: Request, res: Response, ctx: RequestCtx, reqId: string, durationMs: number): void {
  const method = METHODS[req.method] === true ? req.method : "other";
  httpRequests.inc({ route: ctx.route, method, status: String(res.status) });
  httpDuration.observe({ route: ctx.route }, durationMs / 1000);
  if (UNLOGGED_ROUTES[ctx.route] === true) return;
  log.info("request", {
    reqId,
    method,
    route: ctx.route,
    bucket: ctx.bucket,
    status: res.status,
    durationMs: Math.round(durationMs),
  });
}

async function route(deps: ApiDeps, req: Request, server: Server<undefined>, ctx: RequestCtx): Promise<Response> {
  const { store } = deps;
  const url = new URL(req.url);
  if (url.pathname === "/healthz") {
    ctx.route = "healthz";
    return req.method === "GET" ? json(200, { ok: true }) : methodNotAllowed("GET");
  }
  if (url.pathname === "/readyz") {
    ctx.route = "readyz";
    return req.method === "GET" ? readyz(deps) : methodNotAllowed("GET");
  }
  if (url.pathname === "/metrics") {
    ctx.route = "metrics";
    return req.method === "GET" ? metricsText(deps) : methodNotAllowed("GET");
  }
  if (url.pathname.startsWith("/v1/admin/")) return handleAdmin(req, server, deps, ctx);

  const m = OBJECT_PATH.exec(url.pathname);
  if (!m) return notFound();
  const [, rawBucket = "", rawKey] = m;
  let bucket: string;
  let key: string | null;
  try {
    bucket = decodeURIComponent(rawBucket);
    key = rawKey === undefined ? null : decodeURIComponent(rawKey);
  } catch {
    return json(400, { error: "invalid_key" });
  }
  ctx.bucket = bucket;
  ctx.route = objectRoute(req.method, key === null);

  const token = /^bearer (.+)$/i.exec(req.headers.get("authorization") ?? "")?.[1];
  const scope = token === undefined ? null : store.authorize(bucket, token);
  if (scope === null) return json(401, { error: "unauthorized" }, { "WWW-Authenticate": "Bearer" });
  if (scope === "read" && (req.method === "PUT" || req.method === "DELETE")) {
    return json(403, { error: "forbidden" });
  }
  // Every await after auth is bounded by Telegram timeouts, so the 10 s idle close is not needed.
  server.timeout(req, 0);

  if (key === null) {
    if (req.method !== "GET") return methodNotAllowed("GET");
    return listObjects(store, bucket, url);
  }
  if (req.method === "GET" || req.method === "HEAD") return getObject(store, req, bucket, key);
  if (req.method === "PUT") return putObject(store, req, bucket, key);
  if (req.method === "DELETE") return deleteObject(store, req, bucket, key);
  return methodNotAllowed("GET, HEAD, PUT, DELETE");
}

function objectRoute(method: string, list: boolean): string {
  if (list) return "object.list";
  if (method === "GET") return "object.get";
  if (method === "HEAD") return "object.head";
  if (method === "PUT") return "object.put";
  if (method === "DELETE") return "object.delete";
  return "not_found";
}

// Ready when the index answers and at least one enabled bot reached Telegram on its last request.
function readyz(deps: ApiDeps): Response {
  const db = deps.db.ping();
  const bots = deps.pool.health();
  const ok = db && bots.reachable > 0;
  return json(ok ? 200 : 503, { ok, db, bots });
}

// Aggregate values only: no bucket, key or token labels.
function metricsText(deps: ApiDeps): Response {
  const bots = deps.pool.health();
  const queue = deps.batcher.stats();
  const index = deps.db.stats();
  const snap = deps.snapshot.status();
  const gauges: Gauge[] = [
    { name: "tgstore_bots_total", help: "Bot tokens configured.", value: bots.total },
    { name: "tgstore_bots_enabled", help: "Bots not disabled after losing access.", value: bots.enabled },
    { name: "tgstore_bots_reachable", help: "Enabled bots that answered their last request.", value: bots.reachable },
    { name: "tgstore_batcher_queued_bytes", help: "Bytes waiting in the upload batcher.", value: queue.queuedBytes },
    { name: "tgstore_batcher_queued_parts", help: "Parts waiting in the upload batcher.", value: queue.queuedParts },
    { name: "tgstore_objects", help: "Objects in the index.", value: index.objects },
    { name: "tgstore_object_bytes", help: "Bytes of all indexed objects.", value: index.bytes },
    { name: "tgstore_pending_deletes", help: "Telegram messages queued for deletion.", value: index.pendingDeletes },
    {
      name: "tgstore_snapshot_dirty",
      help: "1 when the index has changes not yet snapshotted.",
      value: snap.dirty ? 1 : 0,
    },
    {
      name: "tgstore_snapshot_last_success_timestamp_seconds",
      help: "Unix time of the last successful snapshot; 0 if none.",
      value: (snap.lastSuccessAt ?? 0) / 1000,
    },
  ];
  return new Response(renderMetrics(gauges), { headers: { "Content-Type": "text/plain; version=0.0.4" } });
}

function listObjects(store: Store, bucket: string, url: URL): Response {
  const rawLimit = url.searchParams.get("limit");
  const parsed = rawLimit === null ? Number.NaN : Number.parseInt(rawLimit, 10);
  const limit = Number.isFinite(parsed) ? parsed : undefined;
  const prefix = url.searchParams.get("prefix") ?? "";
  const cursor = url.searchParams.get("cursor");
  return json(200, store.list(bucket, prefix, limit, cursor));
}

async function getObject(store: Store, req: Request, bucket: string, key: string): Promise<Response> {
  const meta = store.head(bucket, key);
  if (!meta) return notFound();
  const ifMatch = parseEtagList(req.headers.get("if-match"));
  if (ifMatch && !ifMatch.includes("*") && !ifMatch.includes(meta.etag)) {
    return json(412, { error: "precondition_failed" });
  }
  const headers = objectHeaders(meta);
  const ifNoneMatch = parseEtagList(req.headers.get("if-none-match"));
  if (ifNoneMatch && (ifNoneMatch.includes("*") || ifNoneMatch.includes(meta.etag))) {
    return new Response(null, { status: 304, headers });
  }
  if (req.method === "HEAD") {
    return new Response(null, { status: 200, headers: { ...headers, "Content-Length": String(meta.size) } });
  }
  const range = parseRange(req.headers.get("range"), meta.size);
  if (range === "unsatisfiable") {
    return json(416, { error: "range_not_satisfiable" }, { "Content-Range": `bytes */${meta.size}` });
  }
  const got = await store.read(bucket, key, range);
  if (!got) return notFound();
  if (range) {
    return new Response(got.body, {
      status: 206,
      headers: { ...headers, "Content-Range": `bytes ${range.start}-${range.end}/${meta.size}` },
    });
  }
  return new Response(got.body, { status: 200, headers });
}

async function putObject(store: Store, req: Request, bucket: string, key: string): Promise<Response> {
  const invalid = keyError(key);
  if (invalid) return json(400, { error: "invalid_key", detail: invalid });
  const metadata = parseMetadata(req.headers);
  if (typeof metadata === "string") return json(400, { error: "invalid_metadata", detail: metadata });
  const rawLength = req.headers.get("content-length");
  const declaredBytes = rawLength !== null && /^\d+$/.test(rawLength) ? Number(rawLength) : null;
  const contentType = req.headers.get("content-type") ?? "application/octet-stream";
  const { created, meta } = await store.put(bucket, key, req.body ?? [], {
    contentType,
    metadata,
    cond: preconditionFrom(req.headers),
    declaredBytes,
  });
  return json(created ? 201 : 200, meta);
}

async function deleteObject(store: Store, req: Request, bucket: string, key: string): Promise<Response> {
  const deleted = store.delete(bucket, key, preconditionFrom(req.headers));
  return deleted ? new Response(null, { status: 204 }) : notFound();
}

function objectHeaders(meta: ObjectMeta): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": meta.contentType,
    ETag: `"${meta.etag}"`,
    "Last-Modified": new Date(meta.updatedAt).toUTCString(),
    "Accept-Ranges": "bytes",
    "X-Content-Type-Options": "nosniff",
  };
  for (const [name, value] of Object.entries(meta.metadata)) headers[`x-meta-${name}`] = value;
  return headers;
}

function preconditionFrom(headers: Headers): Precondition | null {
  const ifMatch = parseEtagList(headers.get("if-match"));
  const ifNoneMatch = parseEtagList(headers.get("if-none-match"));
  return ifMatch || ifNoneMatch ? { ifMatch, ifNoneMatch } : null;
}

// Returns null when the Range header is ignored (malformed, multi-range, or a > b).
// "unsatisfiable" maps to 416.
function parseRange(header: string | null, size: number): ByteSpan | "unsatisfiable" | null {
  if (header === null) return null;
  const m = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!m) return null;
  const [, first = "", last = ""] = m;
  if (first === "" && last === "") return null;
  if (first === "") {
    const suffix = Number(last);
    if (size === 0 || suffix === 0) return "unsatisfiable";
    return { start: Math.max(0, size - suffix), end: size - 1 };
  }
  const start = Number(first);
  if (last !== "" && start > Number(last)) return null;
  if (size === 0 || start >= size) return "unsatisfiable";
  const end = last === "" ? size - 1 : Math.min(Number(last), size - 1);
  return { start, end };
}

// Splits an If-Match / If-None-Match value into bare etags; "*" is kept. Null when absent or empty.
function parseEtagList(header: string | null): string[] | null {
  if (header === null) return null;
  const tags = header
    .split(",")
    .map((tag) => tag.trim().replace(/^W\//, "").replace(/^"(.*)"$/, "$1"))
    .filter((tag) => tag !== "");
  return tags.length > 0 ? tags : null;
}

const META_PREFIX = "x-meta-";
const META_NAME = /^[a-z0-9][a-z0-9-]{0,62}$/;
const META_VALUE = /^[\x20-\x7e]{0,1024}$/;
const META_MAX_ENTRIES = 32;
const META_MAX_JSON_BYTES = 2048;

// Returns the metadata map from x-meta-* headers, or a string describing the first violation.
function parseMetadata(headers: Headers): Record<string, string> | string {
  const metadata: Record<string, string> = {};
  let entries = 0;
  for (const [name, value] of headers) {
    if (!name.startsWith(META_PREFIX)) continue;
    const field = name.slice(META_PREFIX.length);
    if (!META_NAME.test(field)) return `invalid metadata name: ${field}`;
    if (!META_VALUE.test(value)) return `invalid metadata value for ${field}`;
    metadata[field] = value;
    entries++;
    if (entries > META_MAX_ENTRIES) return `more than ${META_MAX_ENTRIES} metadata entries`;
  }
  if (JSON.stringify(metadata).length > META_MAX_JSON_BYTES) {
    return `metadata exceeds ${META_MAX_JSON_BYTES} bytes`;
  }
  return metadata;
}
