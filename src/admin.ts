import type { Server } from "bun";
import { timingSafeEqual } from "node:crypto";
import type { KeyScope } from "./db";
import { hashToken } from "./crypto";
import { BadRequestError, json, methodNotAllowed, notFound, readJsonBody, type RequestCtx } from "./http";
import type { Snapshot } from "./snapshot";
import type { Store } from "./store";

export interface AdminDeps {
  store: Store;
  snapshot: Snapshot;
  // sha256 hex of ADMIN_TOKEN; null when the admin API is disabled.
  adminTokenHash: string | null;
}

const ADMIN_PREFIX = "/v1/admin/";
const MAX_GRACE_SECONDS = 2_592_000;
const DEFAULT_GRACE_SECONDS = 86_400;
const LABEL_RE = /^[\x20-\x7e]{0,64}$/;

// Auth for every admin path. Routes are dispatched by method and path segments.
export async function handleAdmin(
  req: Request,
  server: Server<undefined>,
  deps: AdminDeps,
  ctx: RequestCtx,
): Promise<Response> {
  if (deps.adminTokenHash === null) return notFound();
  const url = new URL(req.url);
  const [top, name, sub, id, extra] = url.pathname.slice(ADMIN_PREFIX.length).split("/");
  if (top === "buckets" && extra === undefined) {
    ctx.route = adminRoute(req.method, name, sub, id);
    ctx.bucket = name ?? null;
  }
  const token = /^bearer (.+)$/i.exec(req.headers.get("authorization") ?? "")?.[1];
  if (token === undefined || !sameHash(hashToken(token), deps.adminTokenHash)) {
    return json(401, { error: "unauthorized" }, { "WWW-Authenticate": "Bearer" });
  }
  // Admin requests can carry large bodies or wait on Telegram; the idle timeout does not apply.
  server.timeout(req, 0);

  if (top !== "buckets" || extra !== undefined) return notFound();
  const method = req.method;

  if (name === undefined) {
    if (method === "GET") return listBuckets(deps);
    if (method === "POST") return createBucket(req, deps);
    return methodNotAllowed("GET, POST");
  }
  if (sub === undefined) {
    return method === "DELETE" ? deleteBucket(deps, name) : methodNotAllowed("DELETE");
  }
  if (sub === "rotate" && id === undefined) {
    return method === "POST" ? rotate(req, deps, name) : methodNotAllowed("POST");
  }
  if (sub === "keys") {
    if (id === undefined) {
      if (method === "GET") return listKeys(deps, name);
      if (method === "POST") return createKey(req, deps, name);
      return methodNotAllowed("GET, POST");
    }
    return method === "DELETE" ? revokeKey(deps, name, id) : methodNotAllowed("DELETE");
  }
  return notFound();
}

// Metric and log label for an admin request, chosen from the path before auth runs.
function adminRoute(
  method: string,
  name: string | undefined,
  sub: string | undefined,
  id: string | undefined,
): string {
  if (name === undefined) return method === "POST" ? "admin.buckets.create" : "admin.buckets.list";
  if (sub === undefined) return "admin.buckets.delete";
  if (sub === "rotate" && id === undefined) return "admin.rotate";
  if (sub === "keys" && id === undefined) return method === "POST" ? "admin.keys.create" : "admin.keys.list";
  if (sub === "keys") return "admin.keys.delete";
  return "not_found";
}

function sameHash(a: string, b: string): boolean {
  return timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

function listBuckets(deps: AdminDeps): Response {
  const buckets = deps.store.listBuckets().map((b) => ({
    name: b.name,
    createdAt: new Date(b.createdAt).toISOString(),
    objectCount: b.objectCount,
    totalBytes: b.totalBytes,
  }));
  return json(200, { buckets });
}

async function createBucket(req: Request, deps: AdminDeps): Promise<Response> {
  const body = await readJsonBody(req);
  const name = field(body, "name");
  if (typeof name !== "string") throw new BadRequestError("name must be a string");
  const label = readLabel(field(body, "label"));
  const issued = deps.store.createBucket(name, label);
  await deps.snapshot.flush();
  return json(201, { bucket: { name, createdAt: issued.createdAt }, key: issued });
}

async function deleteBucket(deps: AdminDeps, name: string): Promise<Response> {
  if (deps.store.deleteBucket(name) < 0) return notFound();
  await deps.snapshot.flush();
  return new Response(null, { status: 204 });
}

function listKeys(deps: AdminDeps, name: string): Response {
  const keys = deps.store.listKeys(name);
  return keys === null ? notFound() : json(200, { keys });
}

async function createKey(req: Request, deps: AdminDeps, name: string): Promise<Response> {
  const body = await readJsonBody(req);
  const scope = readScope(field(body, "scope"));
  const label = readLabel(field(body, "label"));
  const expiresAt = readExpiresAt(field(body, "expiresAt"));
  const issued = deps.store.createKey(name, scope, label, expiresAt);
  if (issued === null) return notFound();
  await deps.snapshot.flush();
  return json(201, { key: issued });
}

async function revokeKey(deps: AdminDeps, name: string, id: string): Promise<Response> {
  if (!deps.store.revokeKey(name, id)) return notFound();
  await deps.snapshot.flush();
  return new Response(null, { status: 204 });
}

async function rotate(req: Request, deps: AdminDeps, name: string): Promise<Response> {
  const body = await readJsonBody(req);
  const graceSeconds = readGraceSeconds(field(body, "graceSeconds"));
  const issued = deps.store.rotateBucket(name, graceSeconds * 1000);
  if (issued === null) return notFound();
  await deps.snapshot.flush();
  return json(201, { key: issued });
}

// Reads one field of a parsed JSON body. A non-object body has no fields.
function field(body: unknown, key: string): unknown {
  if (typeof body !== "object" || body === null) return undefined;
  return Reflect.get(body, key);
}

function readLabel(v: unknown): string {
  if (v === undefined) return "";
  if (typeof v !== "string" || !LABEL_RE.test(v)) {
    throw new BadRequestError("label must be up to 64 printable ASCII characters");
  }
  return v;
}

function readScope(v: unknown): KeyScope {
  if (v === "read" || v === "write") return v;
  throw new BadRequestError('scope must be "read" or "write"');
}

// Absent means the key never expires. Otherwise an ISO timestamp in the future.
function readExpiresAt(v: unknown): number | null {
  if (v === undefined) return null;
  if (typeof v !== "string") throw new BadRequestError("expiresAt must be an ISO timestamp string");
  const ms = Date.parse(v);
  if (Number.isNaN(ms)) throw new BadRequestError("expiresAt must be an ISO timestamp string");
  if (ms <= Date.now()) throw new BadRequestError("expiresAt must be in the future");
  return ms;
}

function readGraceSeconds(v: unknown): number {
  if (v === undefined) return DEFAULT_GRACE_SECONDS;
  if (typeof v !== "number" || !Number.isInteger(v) || v < 0 || v > MAX_GRACE_SECONDS) {
    throw new BadRequestError(`graceSeconds must be an integer between 0 and ${MAX_GRACE_SECONDS}`);
  }
  return v;
}
