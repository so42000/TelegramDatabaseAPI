import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openDb } from "../src/db";

describe("legacy objects table migration", () => {
  test("adds blob ranges (offset 0, length size + 29); reopening is a no-op", () => {
    const dir = mkdtempSync(join(tmpdir(), "tgstore-db-"));
    const path = join(dir, "index.sqlite");
    try {
      const legacy = new Database(path);
      legacy.exec(`
CREATE TABLE buckets (name TEXT PRIMARY KEY, token_hash TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE objects (
  bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
  key TEXT NOT NULL, object_id TEXT NOT NULL, message_id INTEGER NOT NULL, file_id TEXT NOT NULL,
  content_type TEXT NOT NULL, size INTEGER NOT NULL, sha256 TEXT NOT NULL, updated_at INTEGER NOT NULL,
  PRIMARY KEY (bucket, key));
INSERT INTO buckets VALUES ('b', 'h', 1);
INSERT INTO objects VALUES ('b', 'k', 'oid', 7, 'fid', 'text/plain', 10, 'sha', 1);
`);
      legacy.close();

      const db = openDb(path);
      const row = db.getObject("b", "k");
      expect(row?.blobOffset).toBe(0);
      expect(row?.blobLength).toBe(39);
      db.close();

      const again = openDb(path);
      expect(again.getObject("b", "k")?.blobLength).toBe(39);
      again.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
