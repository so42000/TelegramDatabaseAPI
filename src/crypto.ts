import { createHash } from "node:crypto";

const VERSION = 0x01;
const IV_LEN = 12;
const HEADER_LEN = 1 + IV_LEN;
export const BLOB_OVERHEAD = HEADER_LEN + 16;
const encoder = new TextEncoder();

export class DecryptError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DecryptError";
  }
}

export function importMasterKey(raw: Uint8Array<ArrayBuffer>): Promise<CryptoKey> {
  return crypto.subtle.importKey("raw", raw, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

// Layout: [0x01][12-byte IV][ciphertext + 16-byte GCM tag]
export async function encryptBlob(key: CryptoKey, aad: string, plain: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer>> {
  const iv = crypto.getRandomValues(new Uint8Array(IV_LEN));
  const ct = new Uint8Array(
    await crypto.subtle.encrypt({ name: "AES-GCM", iv, additionalData: encoder.encode(aad) }, key, plain),
  );
  const out = new Uint8Array(HEADER_LEN + ct.byteLength);
  out[0] = VERSION;
  out.set(iv, 1);
  out.set(ct, HEADER_LEN);
  return out;
}

export async function decryptBlob(key: CryptoKey, aad: string, blob: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer>> {
  if (blob.byteLength < BLOB_OVERHEAD) throw new DecryptError("blob too short");
  if (blob[0] !== VERSION) throw new DecryptError("unsupported blob version");
  try {
    const plain = await crypto.subtle.decrypt(
      { name: "AES-GCM", iv: blob.subarray(1, HEADER_LEN), additionalData: encoder.encode(aad) },
      key,
      blob.subarray(HEADER_LEN),
    );
    return new Uint8Array(plain);
  } catch {
    throw new DecryptError("authentication failed");
  }
}

export function sha256Hex(data: Uint8Array | string): string {
  return createHash("sha256").update(data).digest("hex");
}

export function newBucketToken(): string {
  return "tgs_" + Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64url");
}

export function hashToken(token: string): string {
  return sha256Hex(token);
}

export function partAad(objectId: string, partNo: number): string {
  return partNo === 0 ? objectId : `${objectId}:${partNo}`;
}

export function newKeyId(): string {
  return "key_" + Buffer.from(crypto.getRandomValues(new Uint8Array(8))).toString("hex");
}
