# tg-storage-api

Self-hosted object store backed by a private Telegram channel. Objects are encrypted with AES-256-GCM and stored as documents. A small SQLite index maps keys to Telegram file ids. An encrypted snapshot of the index is pinned in the channel, so an empty volume can rebuild itself from Telegram.

Your servers talk to an HTTP API with a per-bucket password. The owner manages buckets and uploads files through a Telegram bot.

> **Risk:** Telegram's Bot Developer Terms §5.2(e) forbid using bots as cloud storage. Using this may get the bot banned. Your data stays in your channel and is encrypted, but keep `MASTER_KEY` and periodic exports safe.

## Requirements

- Bun 1.4.5 (or Docker with `oven/bun:1.4.5`)
- A Telegram account that will own a private channel
- A Telegram bot token from @BotFather

## 1. Create the bot and channel

1. In Telegram, message @BotFather, send `/newbot`, and copy the token. This is `TELEGRAM_BOT_TOKENS`. For more write throughput, create more bots and add them to the storage channel as admins with Post, Edit and Delete rights; list all tokens comma-separated.
2. Create a **private channel**. Add your bot as an administrator with **Post Messages**, **Edit Messages** and **Delete Messages** rights.
3. Find the IDs:
   1. Post any message in the channel.
   2. Send any message to your bot.
   3. Run `curl -s "https://api.telegram.org/bot<TOKEN>/getUpdates"`.
   4. `channel_post.chat.id` is `STORAGE_CHANNEL_ID` (looks like `-100...`).
   5. `message.from.id` is your user id, used for `OWNER_IDS`.

## 2. Configure

```sh
cp .env.example .env
```

Generate a master key and put it in `MASTER_KEY`:

```sh
bun -e 'console.log(Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64"))'
```

Keep this key safe and outside the container. Without it, the encrypted snapshot and exports cannot be read.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `TELEGRAM_BOT_TOKENS` | yes | | Comma-separated bot tokens from BotFather; each bot must be a channel admin and receive `/start` from the owner |
| `STORAGE_CHANNEL_ID` | yes | | Private channel id, e.g. `-1001234567890` |
| `OWNER_IDS` | yes | | Comma-separated Telegram user ids allowed to use the bot |
| `MASTER_KEY` | yes | | Base64 of exactly 32 random bytes |
| `PORT` | no | `8080` | HTTP port (`0` = random) |
| `DB_PATH` | no | `./data/index.sqlite` | SQLite index location |
| `TELEGRAM_API_BASE` | no | `https://api.telegram.org` | Bot API base URL |
| `SNAPSHOT_INTERVAL_MS` | no | `60000` | Index snapshot interval |
| `CHANNEL_MIN_INTERVAL_MS` | no | `3000` | Minimum gap between channel writes; raise to `4000` if you see repeated 429s |
| `QUEUE_MAX_BYTES` | no | `67108864` | Pending PUT bytes waiting for the channel before the API returns 503 |
| `BOT_POLLING` | no | `true` | Set `false` to disable the bot |

## 3. Run

### With Docker (recommended)

```sh
docker compose up -d --build
docker compose logs -f tgstore
```

Expected log lines on first start: `restore: no snapshot pinned, starting empty` and `listening on :8080`. Data lives in `./data`.

### Without Docker

```sh
bun install
bun run start
```

## 4. Use

In a private chat with the bot (from an owner account):

```
/newbucket myapp        -> replies with a tgs_... password (shown once)
/rotate myapp           -> new password for the bucket
/buckets                -> list buckets
/ls myapp users/        -> list keys
/get myapp users/1      -> send the object as a file
/rm myapp users/1       -> delete one object
/delbucket myapp        -> asks for confirmation
/delbucket myapp yes    -> deletes the bucket and its objects
```

Upload by sending a file (or photo) with the caption `<bucket> <key>`, for example `myapp pics/cat`. Send photos as files to keep original quality. Maximum object size is 19 MB.

### HTTP API

Use the bucket password as a bearer token from servers only. There is no CORS.

```sh
export PW=tgs_...   # from /newbucket
BASE=http://localhost:8080/v1/buckets/myapp/objects

# write (201 new, 200 replaced)
curl -s -X PUT -H "Authorization: Bearer $PW" -H "Content-Type: application/json" \
  --data '{"name":"Alice"}' $BASE/users/1

# read
curl -s -H "Authorization: Bearer $PW" $BASE/users/1

# list with paging
curl -s -H "Authorization: Bearer $PW" "$BASE?prefix=users/&limit=50"

# delete (204)
curl -s -X DELETE -H "Authorization: Bearer $PW" $BASE/users/1
```

Health check (no auth): `GET /healthz`.

## 5. Export a backup

```sh
docker compose exec tgstore bun run export /data/backup.ndjson
```

Each line is one object: `bucket`, `key`, `contentType`, `size`, `sha256`, `updatedAt`, and `data` (base64). Failed objects are written with an `error` field, and the command exits 1 if any failed. Export can run while the service is up.

## 6. Restore

If the volume is lost, start with an empty `DB_PATH` and the same `MASTER_KEY` and channel. The service reads the pinned snapshot and logs `restore: restored N buckets, M objects from snapshot`. A wrong `MASTER_KEY` makes startup fail with `snapshot decrypt failed`, so the index is never overwritten.

## Development

```sh
bun install
bun run typecheck
bun test          # uses a fake Telegram server; no network or token needed
```

Layout: `src/config.ts`, `crypto.ts`, `telegram.ts` (Bot API client), `db.ts` (SQLite index), `store.ts` (object logic), `snapshot.ts` (index snapshot and restore), `api.ts` (HTTP), `bot.ts` (owner commands), `app.ts` (wiring), `main.ts` (entry), `cli/export.ts`.

## Limitations

- Snapshot lag: if the volume is lost without a graceful stop, changes from the last `SNAPSHOT_INTERVAL_MS` are lost. Bucket create, rotate and delete flush immediately.
- Telegram Bot API limits: downloads up to 20 MB, uploads up to 50 MB. Objects above 19 MB are rejected.
- Channel writes are rate-limited (assumed 20 per minute).
