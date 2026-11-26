import { errorText, log } from "./log";
import { TelegramError } from "./telegram";
import { QueueFullError } from "./batcher";
import {
  BodyMismatchError,
  BucketExistsError,
  IntegrityError,
  InvalidNameError,
  NoSuchBucketError,
  PreconditionFailedError,
  TooLargeError,
  type Store,
} from "./store";

// Filled in by the router so the wrapper can label metrics and the request log.
export interface RequestCtx {
  route: string;
  bucket: string | null;
}

export function json(status: number, body: unknown, headers: HeadersInit = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

export function notFound(): Response {
  return json(404, { error: "not_found" });
}

export function methodNotAllowed(allow: string): Response {
  return json(405, { error: "method_not_allowed" }, { Allow: allow });
}

// Client input that is well-formed HTTP but wrong for the endpoint. Mapped to 400 invalid_request.
export class BadRequestError extends Error {
  name = "BadRequestError";
}

export class PayloadTooLargeError extends Error {
  name = "PayloadTooLargeError";
  readonly maxBytes: number;

  constructor(maxBytes: number) {
    super(`payload exceeds ${maxBytes} bytes`);
    this.maxBytes = maxBytes;
  }
}

export function errorResponse(store: Store, err: unknown): Response {
  if (err instanceof TooLargeError) return json(413, { error: "too_large", maxBytes: store.maxObjectBytes });
  if (err instanceof PayloadTooLargeError) return json(413, { error: "too_large", maxBytes: err.maxBytes });
  if (err instanceof BadRequestError) return json(400, { error: "invalid_request", detail: err.message });
  if (err instanceof BodyMismatchError) {
    return json(400, { error: "invalid_request", detail: "body length does not match Content-Length" });
  }
  if (err instanceof PreconditionFailedError) return json(412, { error: "precondition_failed" });
  if (err instanceof InvalidNameError) return json(400, { error: "invalid_name" });
  if (err instanceof BucketExistsError) return json(409, { error: "bucket_exists" });
  if (err instanceof QueueFullError) return json(503, { error: "busy" }, { "Retry-After": "30" });
  if (err instanceof NoSuchBucketError) return notFound();
  if (err instanceof TelegramError) return json(502, { error: "telegram_error", detail: err.description });
  if (err instanceof IntegrityError) return json(500, { error: "integrity" });
  log.error("unhandled request error", { err: errorText(err), stack: err instanceof Error ? err.stack : undefined });
  return json(500, { error: "internal" });
}

// Parses a JSON body of at most maxBytes. An empty body parses as {}; callers validate the shape.
export async function readJsonBody(req: Request, maxBytes = 65_536): Promise<unknown> {
  const declared = req.headers.get("content-length");
  if (declared !== null && Number(declared) > maxBytes) throw new PayloadTooLargeError(maxBytes);
  const chunks: Uint8Array[] = [];
  let total = 0;
  if (req.body) {
    const reader = req.body.getReader();
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      total += chunk.value.byteLength;
      if (total > maxBytes) {
        await reader.cancel();
        throw new PayloadTooLargeError(maxBytes);
      }
      chunks.push(chunk.value);
    }
  }
  if (total === 0) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new BadRequestError("body is not valid JSON");
  }
}
