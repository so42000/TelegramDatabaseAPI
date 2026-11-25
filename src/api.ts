import { TelegramError } from "./telegram";
import {
  IntegrityError,
  keyError,
  MAX_OBJECT_BYTES,
  NoSuchBucketError,
  QueueFullError,
  Store,
  TooLargeError,
} from "./store";

const OBJECT_PATH = /^\/v1\/buckets\/([^/]+)\/objects(?:\/(.+))?$/;

function json(status: number, body: unknown, headers: HeadersInit = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

function notFound(): Response {
  return json(404, { error: "not_found" });
}

function methodNotAllowed(allow: string): Response {
  return json(405, { error: "method_not_allowed" }, { Allow: allow });
}

function errorResponse(err: unknown): Response {
  if (err instanceof TooLargeError) return json(413, { error: "too_large", maxBytes: MAX_OBJECT_BYTES });
  if (err instanceof QueueFullError) return json(503, { error: "busy" }, { "Retry-After": "30" });
  if (err instanceof NoSuchBucketError) return notFound();
  if (err instanceof TelegramError) return json(502, { error: "telegram_error", detail: err.description });
  if (err instanceof IntegrityError) return json(500, { error: "integrity" });
  console.error(err);
  return json(500, { error: "internal" });
}

export function createApiHandler(store: Store): (req: Request) => Promise<Response> {
  return async (req) => {
    try {
      return await route(store, req);
    } catch (err) {
      return errorResponse(err);
    }
  };
}

async function route(store: Store, req: Request): Promise<Response> {
  const url = new URL(req.url);
  if (url.pathname === "/healthz") {
    return req.method === "GET" ? json(200, { ok: true }) : methodNotAllowed("GET");
  }

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

  const auth = /^bearer (.+)$/i.exec(req.headers.get("authorization") ?? "");
  if (!auth || !store.authorize(bucket, auth[1] ?? "")) {
    return json(401, { error: "unauthorized" }, { "WWW-Authenticate": "Bearer" });
  }

  if (key === null) {
    if (req.method !== "GET") return methodNotAllowed("GET");
    return listObjects(store, bucket, url);
  }
  if (req.method === "GET") return getObject(store, bucket, key);
  if (req.method === "PUT") return putObject(store, req, bucket, key);
  if (req.method === "DELETE") return deleteObject(store, bucket, key);
  return methodNotAllowed("GET, PUT, DELETE");
}

function listObjects(store: Store, bucket: string, url: URL): Response {
  const rawLimit = url.searchParams.get("limit");
  const parsed = rawLimit === null ? Number.NaN : Number.parseInt(rawLimit, 10);
  const limit = Number.isFinite(parsed) ? parsed : undefined;
  const prefix = url.searchParams.get("prefix") ?? "";
  const cursor = url.searchParams.get("cursor");
  return json(200, store.list(bucket, prefix, limit, cursor));
}

async function getObject(store: Store, bucket: string, key: string): Promise<Response> {
  const got = await store.get(bucket, key);
  if (!got) return notFound();
  return new Response(got.body, {
    status: 200,
    headers: { "Content-Type": got.meta.contentType, ETag: `"${got.meta.etag}"` },
  });
}

async function putObject(store: Store, req: Request, bucket: string, key: string): Promise<Response> {
  const invalid = keyError(key);
  if (invalid) return json(400, { error: "invalid_key", detail: invalid });
  const declared = Number(req.headers.get("content-length") ?? 0);
  if (declared > MAX_OBJECT_BYTES) return json(413, { error: "too_large", maxBytes: MAX_OBJECT_BYTES });

  const body = new Uint8Array(await req.arrayBuffer());
  const contentType = req.headers.get("content-type") ?? "application/octet-stream";
  const { created, meta } = await store.put(bucket, key, body, contentType);
  return json(created ? 201 : 200, meta);
}

async function deleteObject(store: Store, bucket: string, key: string): Promise<Response> {
  return (await store.delete(bucket, key)) ? new Response(null, { status: 204 }) : notFound();
}
