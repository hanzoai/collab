<p align="center"><img src=".github/hero.svg" alt="collab" width="880"></p>

# collab

Hanzo realtime collaboration relay. Replaces Huly's `collaborator`
Node service with a single static Go binary.

```
clients (Y.js) ──WS──> collab ──Append──> SQLite
                  └─ fan-out to peers in the same room
```

## API

| Method | Path                  | Notes |
|--------|-----------------------|-------|
| GET    | `/v1/health`          | `{"status":"ok","version":"..."}` |
| WS     | `/v1/collab/<doc_id>` | Subprotocol `yjs`. Binary frames relayed + persisted. |
| GET    | `/v1/metrics`         | Prometheus exposition. |

`doc_id` MUST be `<orgID>:<workspaceID>:<docID>`. The IAM JWT's
`owner` claim must equal `<orgID>` or the upgrade is rejected with 403.

### Auth

Bearer JWT in `Authorization: Bearer <token>`, or, from a browser (whose
WebSocket API cannot set headers), as a `bearer.<token>` entry in the
subprotocol list beside `yjs`:

```js
new WebSocket(url, ["yjs", "bearer." + token])
```

A `?token=` query parameter is not read: the edge writes query strings to
its access log. Tokens are verified against the IAM JWKS at `IAM_JWKS_URL`
(default `https://hanzo.id/v1/iam/.well-known/jwks`); when a refresh fails,
the keys of the last good fetch keep verifying.

## Sync protocol

The server is a dumb relay — it does NOT merge Y.js updates. Every
binary frame is relayed to the other peers in the room. A frame is
stored when its y-websocket header says it carries document content
(`messageSync` followed by `syncStep2` or `syncUpdate`); sync step 1 and
awareness are relayed only. Applying the stored updates in order
rebuilds the document, so persistence is append-only frames. We revisit
this only when cross-region merge or server-side validation is required.

On peer join the server writes each stored frame as its own binary
message, in order, before relaying live updates.

### Limits

| Limit | Value | On breach |
|-------|-------|-----------|
| Frame size | 1 MiB | connection closed, 1009 |
| Frame rate per connection | 50/s, burst 200 | connection closed, 1008 |
| Stored bytes per document | 16 MiB | frame relayed, not stored (`store_full`) |
| Database file | `COLLAB_STORE_BYTES` | frame relayed, not stored (`store_append`) |

Each connection has at most one append in flight: it reads its next
frame only after the last one is stored. The database runs a rollback
journal, which holds only the pages one transaction changes, so the
files on disk never exceed twice `COLLAB_STORE_BYTES`. Size the volume
from that, not the other way round.

## Run

```
go run ./cmd/collab
```

| Env                  | Default                                  |
|----------------------|------------------------------------------|
| `COLLAB_ADDR`        | `:3078` (Huly drop-in port)              |
| `COLLAB_SQLITE_PATH` | `collab.db` (a file path)                |
| `COLLAB_STORE_BYTES` | `67108864` (64 MiB database file cap)    |
| `IAM_JWKS_URL`       | `https://hanzo.id/v1/iam/.well-known/jwks`      |

## Build

```
go build ./...
go test  ./...
```

Docker (CI/CD only — never build on a laptop, see hanzoai/.github):

```
docker build --build-arg VERSION=v0.1.0 -t ghcr.io/hanzoai/collab:v0.1.0 .
```

## Deploy

Image: `ghcr.io/hanzoai/collab:<semver>`. Pin a `vX.Y.Z` or
`sha-<sha7>` tag — never `:latest`, `:main`, `:dev`.

Port 3078 is exposed (matches Huly's legacy `collaborator` port for
drop-in WS path replacement).

Declared in universe as `charts/app/values/collab/collab.yaml`, with
SQLite on one ReadWriteOnce volume, one replica, Recreate.

## Layout

```
.
├── cmd/collab/main.go        # boot + signal + env
├── pkg/auth/                 # IAM JWKS verifier
├── pkg/server/               # HTTP + WS handlers
├── pkg/room/                 # in-memory broadcast registry
├── pkg/store/                # Store interface + sqlite
└── pkg/metrics/              # Prometheus registry
```

Licensed under **MIT OR Apache-2.0**, per [HIP-0137](https://github.com/hanzoai/hips/blob/main/HIPs/hip-0137-one-license.md).
