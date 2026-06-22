# Redis HTTP Bridge

A ~2 MB Docker image (UPX-compressed scratch binary) that bridges HTTP to Redis. Use with Postman to interact with any Redis instance by passing the Redis URL as a query param.

> Intended for **local / personal use** only. The Redis URL (with credentials) is passed in the query string. Do not expose `:8087` outside your machine.

## Quick Start

```bash
docker build -t redis-bridge:v1 .
docker run -d -p 127.0.0.1:8087:8087 --restart=always --name redis-bridge-c redis-bridge:v1
```

Always available at `http://localhost:8087`.

### Run without Docker

```bash
go run main.go
```

Requires Go 1.26+.

## Configuration

All settings are optional. Defaults are tuned for local use.

| Env var | Default | Purpose |
|---|---|---|
| `REDIS_BRIDGE_ADDR` | `:8087` | HTTP listen address. Use `127.0.0.1:8087` to bind loopback only. |
| `REDIS_BRIDGE_MAX_BODY` | `10485760` | Max request body size in bytes (10 MB). |
| `REDIS_BRIDGE_CACHE_SIZE` | `32` | Max number of pooled Redis clients (LRU-bounded). |

## API

All requests go to `/redis`. Query params: `url` (redis connection), `key`, and optional `ttl` (seconds).
The request body **is** the value — raw content, no JSON wrapping.

### Create
```
POST /redis?url={{redis_url}}&key={{key}}&ttl=3600

Body (raw): your value (YAML, JSON, HTML, plain text, etc.)
```

| Status | Meaning |
|---|---|
| `201 Created` | Key created. |
| `400 Bad Request` | Missing `url`/`key`, invalid `ttl`, empty body, or invalid Redis URL. |
| `409 Conflict` | Key already exists. |
| `413 Request Entity Too Large` | Body exceeds `REDIS_BRIDGE_MAX_BODY`. |
| `503 Service Unavailable` | Redis-side failure (see container logs for details). |

### Read
```
GET /redis?url={{redis_url}}&key={{key}}

Response headers:
  X-Redis-TTL: 3200  (seconds remaining, or -1 if no expiry)

Response body: raw value as stored
```

| Status | Meaning |
|---|---|
| `200 OK` | Returns raw value; `X-Redis-TTL` header set. |
| `400 Bad Request` | Missing `url` or `key`. |
| `404 Not Found` | Key does not exist. |
| `503 Service Unavailable` | Redis-side failure. |

### Update
```
PUT /redis?url={{redis_url}}&key={{key}}&ttl=7200

Body (raw): updated value
```

| Status | Meaning |
|---|---|
| `204 No Content` | Key updated. |
| `400 Bad Request` | Missing `url`/`key`, invalid `ttl`, empty body, or invalid Redis URL. |
| `404 Not Found` | Key does not exist (use POST to create). |
| `413 Request Entity Too Large` | Body exceeds `REDIS_BRIDGE_MAX_BODY`. |
| `503 Service Unavailable` | Redis-side failure. |

### Update TTL only
```
PATCH /redis?url={{redis_url}}&key={{key}}&ttl=86400
```

| Status | Meaning |
|---|---|
| `204 No Content` | TTL updated. |
| `400 Bad Request` | Missing/invalid `ttl` (must be > 0), or missing `url`/`key`. |
| `404 Not Found` | Key does not exist. |
| `503 Service Unavailable` | Redis-side failure. |

### Delete
```
DELETE /redis?url={{redis_url}}&key={{key}}
```

| Status | Meaning |
|---|---|
| `204 No Content` | Key deleted. |
| `400 Bad Request` | Missing `url` or `key`. |
| `404 Not Found` | Key does not exist. |
| `503 Service Unavailable` | Redis-side failure. |

### TTL semantics

- Omit `ttl` to store without expiration (POST/PUT only).
- Pass `ttl=N` (positive integer seconds) to set an N-second expiration.
- Invalid `ttl` (non-numeric or `<= 0`) is rejected with `400` on every verb.
- `X-Redis-TTL: -1` on read means "key has no expiration".

## URL Format

Standard Redis URL: `redis://[username:password@]host:port[/db]`. TLS via `rediss://` is supported (handled by the underlying client).

## Postman Setup

1. Create environments (`stage-us`, `stage-ir`, `prod-us`, etc.) with variables:
   - `redis_url` = your Redis connection URL
   - `key` = your Redis key

2. Use in requests:
   ```
   GET http://localhost:8087/redis?url={{redis_url}}&key={{key}}
   ```

3. Switch environment dropdown to target different Redis instances.

## Logs

Logs go to stderr with leveled prefixes (`INFO`/`ERROR`). Each request emits one line with `method`, `path`, `key`, `status`, and `dur`. Redis-side failures emit a separate `ERROR` line with `op`, `key`, and the underlying error.

```bash
docker logs -f redis-bridge-c
```

## Docker Management

```bash
docker stop redis-bridge-c     # stop
docker start redis-bridge-c    # start
docker logs redis-bridge-c     # logs
docker rm -f redis-bridge-c    # remove
```
