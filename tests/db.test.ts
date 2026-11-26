import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openDb } from "../src/db";

describe("legacy schema migration to v4", () => {
  test("turns a legacy object into part 0 and its bucket token into one write key; reopening is a no-op", () => {
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
      expect(db.getParts("oid")).toEqual([
        { objectId: "oid", partNo: 0, messageId: 7, fileId: "fid", botId: 0, blobOffset: 0, blobLength: 39, size: 10 },
      ]);
      const keys = db.listKeys("b");
      expect(keys.length).toBe(1);
      expect(keys[0]).toMatchObject({ bucket: "b", tokenHash: "h", scope: "write", label: "migrated" });
      db.close();

      const raw = new Database(path, { readonly: true });
      const columns = (raw.query("PRAGMA table_info(objects)").all() as { name: string }[]).map((c) => c.name);
      expect(columns).not.toContain("message_id");
      expect(raw.query("PRAGMA user_version").get()).toEqual({ user_version: 4 });
      raw.close();

      const again = openDb(path);
      expect(again.listKeys("b").length).toBe(1);
      expect(again.getParts("oid").length).toBe(1);
      again.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
