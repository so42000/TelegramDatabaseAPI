import { existsSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { errorText } from "../log";

// Import is a client of the HTTP API: it creates buckets and temporary write keys through the admin API and
// writes objects with those keys, so it works against any reachable service, not only the local DB.
const USAGE = "usage: ADMIN_TOKEN=<token> bun run import <source> <baseUrl>";
const sourceArg = process.argv[2];
const baseArg = process.argv[3];
const adminToken = process.env.ADMIN_TOKEN;
if (!sourceArg || !baseArg || !adminToken) {
  console.error(USAGE);
  process.exit(2);
}
if (!existsSync(sourceArg)) {
  console.error(`import: ${sourceArg} does not exist`);
  process.exit(2);
}

const source = resolve(sourceArg);
const base = baseArg.replace(/\/+$/, "");
// A directory is an export (index.ndjson plus blobs); a file is a legacy .ndjson whose lines carry base64 data.
const dirMode = statSync(source).isDirectory();
const indexPath = dirMode ? join(source, "index.ndjson") : source;
if (!existsSync(indexPath)) {
  console.error(`import: ${indexPath} does not exist`);
  process.exit(2);
}

const CONCURRENCY = 4;
const MAX_RETRIES = 20;
const DEFAULT_RETRY_S = 5;
const KEY_TTL_MS = 24 * 60 * 60 * 1000;

interface Entry {
  bucket: string;
  key: string;
  contentType?: string;
  size?: number;
  sha256?: string;
  metadata?: Record<string, string>;
  file?: string;
  data?: string;
  error?: string;
}

const stats = { imported: 0, skipped: 0, failed: 0 };
const tempKeys: { bucket: string; id: string }[] = [];
const bucketTokens = new Map<string, Promise<string>>();

function adminFetch(method: string, path: string, body?: unknown): Promise<Response> {
  return fetch(`${base}${path}`, {
    method,
    headers: { Authorization: `Bearer ${adminToken}`, "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

function objectUrl(bucket: string, key: string): string {
  return `${base}/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`;
}

// Creates the bucket on first sight (409 means it already exists) and returns a temporary write key for it.
// Memoized per bucket so concurrent objects share one setup.
function writeTokenFor(bucket: string): Promise<string> {
  let token = bucketTokens.get(bucket);
  if (!token) {
    token = openBucket(bucket);
    bucketTokens.set(bucket, token);
  }
  return token;
}

async function openBucket(bucket: string): Promise<string> {
  const created = await adminFetch("POST", "/v1/admin/buckets", { name: bucket });
  if (created.status === 201) {
    const body: { key: { token: string } } = await created.json();
    console.log(`created bucket ${bucket}; password (shown once): ${body.key.token}`);
  } else {
    const text = await created.text();
    if (created.status !== 409) throw new Error(`create bucket ${bucket}: ${created.status} ${text}`);
  }
  const res = await adminFetch("POST", `/v1/admin/buckets/${encodeURIComponent(bucket)}/keys`, {
    scope: "write",
    label: "import",
    expiresAt: new Date(Date.now() + KEY_TTL_MS).toISOString(),
  });
  const text = await res.text();
  if (res.status !== 201) throw new Error(`create import key for ${bucket}: ${res.status} ${text}`);
  const body: { key: { id: string; token: string } } = JSON.parse(text);
  tempKeys.push({ bucket, id: body.key.id });
  return body.key.token;
}

async function importObject(entry: Entry): Promise<"imported" | "skipped"> {
  if (entry.error !== undefined) throw new Error(`export failed: ${entry.error}`);
  const sha = entry.sha256;
  if (sha === undefined) throw new Error("index line has no sha256");
  const token = await writeTokenFor(entry.bucket);
  const url = objectUrl(entry.bucket, entry.key);
  const auth = { Authorization: `Bearer ${token}` };

  const head = await fetch(url, { method: "HEAD", headers: auth });
  if (head.status === 200 && head.headers.get("etag") === `"${sha}"`) return "skipped";

  let body: Blob | Uint8Array<ArrayBuffer>;
  if (dirMode) {
    if (entry.file === undefined) throw new Error("index line has no file");
    body = Bun.file(join(source, entry.file));
  } else {
    if (entry.data === undefined) throw new Error("legacy line has no data");
    body = new Uint8Array(Buffer.from(entry.data, "base64"));
  }
  const headers: Record<string, string> = {
    ...auth,
    "Content-Type": entry.contentType ?? "application/octet-stream",
  };
  for (const [name, value] of Object.entries(entry.metadata ?? {})) headers[`x-meta-${name}`] = value;

  for (let attempt = 0; ; attempt++) {
    const res = await fetch(url, { method: "PUT", headers, body });
    if (res.status === 503 && attempt < MAX_RETRIES) {
      await res.body?.cancel();
      const retryAfter = Number(res.headers.get("retry-after"));
      await Bun.sleep((retryAfter > 0 ? retryAfter : DEFAULT_RETRY_S) * 1000);
      continue;
    }
    const text = await res.text();
    if (res.status !== 200 && res.status !== 201) throw new Error(`${res.status} ${text}`);
    const meta: { etag?: string } = JSON.parse(text);
    if (meta.etag !== sha) throw new Error(`${res.status} etag ${meta.etag} does not match sha256 ${sha}`);
    return "imported";
  }
}

async function handle(line: string): Promise<void> {
  let entry: Entry;
  try {
    entry = JSON.parse(line);
  } catch (err) {
    stats.failed++;
    console.error(`failed line: ${errorText(err)}`);
    return;
  }
  const label = `${entry.bucket}/${entry.key}`;
  try {
    stats[await importObject(entry)]++;
  } catch (err) {
    stats.failed++;
    console.error(`failed ${label}: ${errorText(err)}`);
  }
}

// Streams lines without loading the whole file: legacy exports can hold very large single lines.
async function* readLines(path: string): AsyncGenerator<string> {
  let buffered = "";
  for await (const chunk of Bun.file(path).stream().pipeThrough(new TextDecoderStream())) {
    buffered += chunk;
    let nl = buffered.indexOf("\n");
    while (nl !== -1) {
      yield buffered.slice(0, nl);
      buffered = buffered.slice(nl + 1);
      nl = buffered.indexOf("\n");
    }
  }
  if (buffered.length > 0) yield buffered;
}

const active = new Set<Promise<void>>();
try {
  for await (const line of readLines(indexPath)) {
    if (line.trim() === "") continue;
    const task: Promise<void> = handle(line).finally(() => active.delete(task));
    active.add(task);
    if (active.size >= CONCURRENCY) await Promise.race(active);
  }
  await Promise.all(active);
} finally {
  for (const { bucket, id } of tempKeys) {
    const res = await adminFetch("DELETE", `/v1/admin/buckets/${encodeURIComponent(bucket)}/keys/${id}`);
    if (res.status !== 204) console.error(`warning: could not delete import key ${id} for ${bucket}: ${res.status}`);
  }
}

console.log(`imported ${stats.imported}, skipped ${stats.skipped}, failed ${stats.failed}`);
process.exit(stats.failed > 0 ? 1 : 0);
