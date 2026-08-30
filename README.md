# Computer Communications and Cloud Computing Principles (01418351)

Youtube: https://www.youtube.com/watch?v=axsgPjyPfSk <br>

นายป้อมเดช ฟุง <br>
6710451046 <br>
sec 200 <br>

# Bid Auction Protocol (TCP)
Client, Registry, Proxy, Server
```
Client ──lookup──▶ Registry ──▶ tells client which proxy to use
Client ──bid─────▶ Proxy N ────▶ Server N (owns state, writes audit log + snapshot)
```

## Overall
- **Dynamic proxies**: proxies are no longer listed in a shared, compiled-in
  config. Each proxy is started with its own flags (`-id -low -high -port
  -server-port`) and registers itself with the `registry` on startup. Add a
  new proxy — a new range, or another instance of an existing range — at any
  time without touching any other file.
- **Durability via audit log + snapshot**: each `server` writes every bid
  decision to `server_<id>_audit.log` (append-only, plain text) and rewrites
  `server_<id>_snapshot.txt` (current state) after every accepted bid. On
  restart, the server reloads its snapshot automatically — a crash no longer
  wipes that shard's data.
- **Startup lock prevents duplicate-shard conflicts**: each `server` claims
  `server_<id>.lock` (containing its own PID) before it starts. If a second
  process is accidentally started for the same shard ID, it fails immediately
  with a clear error instead of both processes racing to overwrite the same
  audit log and snapshot files. If the previous holder crashed without
  cleaning up, the stale lock (PID no longer running) is detected and
  reclaimed automatically on the next startup.

| Component | Role | Address |
|---|---|---|
| `registry` | Proxies register here; clients look up by item ID. | `0.0.0.0:8000` |
| `server -id 1` | Owns state for items 1–100. Writes audit log + snapshot. | `0.0.0.0:6001` |
| `server -id 2` | Owns state for items 101–200. Writes audit log + snapshot. | `0.0.0.0:6002` |
| `proxy -id 1` | Validates items 1–100, forwards to server 1, registers itself. | `127.0.0.1:7001` |
| `proxy -id 2` | Validates items 101–200, forwards to server 2, registers itself. | `127.0.0.1:7002` |
| `client` | Looks up the right proxy in the registry, then places a bid. | connects out |

## Protocol
**Client/Proxy ↔ Registry** — newline-delimited JSON:
```json
{"type": "register", "proxy_id": 1, "low": 1, "high": 100, "proxy_host": "127.0.0.1", "proxy_port": "7001"}
{"type": "lookup", "item_id": 42}
```
```json
{"status": "ok", "proxy_id": 1, "proxy_host": "127.0.0.1", "proxy_port": "7001"}
{"status": "not_found", "reason": "no proxy registered for item 999"}
```

**Client → Proxy → Server** — unchanged:
```json
{"client_name": "Alice", "item_id": 42, "bid_amount": 100.0}
```
```json
{"status": "accepted", "item_id": 42, "highest_bid": 100.0, "highest_bidder": "Alice"}
```

## Build & run
From `auction-go/`, 6 terminals for a 2-shard setup (registry, 2 servers, 2
proxies, 1+ clients):

```bash
go build -o bin/registry ./registry
go build -o bin/server   ./server
go build -o bin/proxy    ./pkg/proxy
go build -o bin/client   ./client

# 1. Registry (start first — proxies register with it on startup)
./bin/registry

# 2. Shard 1
./bin/server -id 1 -port 6001 -low 1   -high 100
./bin/proxy  -id 1 -low 1   -high 100 -port 7001 -server-port 6001

# 3. Shard 2
./bin/server -id 2 -port 6002 -low 101 -high 200
./bin/proxy  -id 2 -low 101 -high 200 -port 7002 -server-port 6002

# 4. Client(s)
./bin/client -name Alice -item 42  -amount 100
./bin/client -name Bob   -item 42  -amount 150
./bin/client -name Dave  -item 150 -amount 500
```

Or `go run` each directly with the same flags. Run `./bin/client` with no
flags for an interactive prompt.

## What lands on disk
Each component writes into its own subdirectory under `data/`, created
automatically on first run — no manual setup needed:
```
data/
├── registry/
│   └── registry_audit.log
├── server_1/
│   ├── server_1_audit.log
│   ├── server_1_snapshot.txt
│   └── server_1.lock
└── server_2/
    ├── server_2_audit.log
    ├── server_2_snapshot.txt
    └── server_2.lock
```

After a few bids:
```
data/server_1/server_1_audit.log
2026-08-25 06:20:11 | ACCEPTED | item=42 | bidder=Alice | amount=100.00
2026-08-25 06:20:14 | ACCEPTED | item=42 | bidder=Bob   | amount=150.00
2026-08-25 06:20:17 | REJECTED | item=42 | bidder=Carol | amount=120.00 | current_highest=150.00 by Bob

data/server_1/server_1_snapshot.txt
item=42 | highest_bid=150.00 | bidder=Bob

data/registry/registry_audit.log
2026-08-25 06:19:50 | REGISTER | proxy=1 | range=1-100   | addr=127.0.0.1:7001
2026-08-25 06:19:52 | REGISTER | proxy=2 | range=101-200 | addr=127.0.0.1:7002
```

Try killing `server -id 1` mid-run and restarting it with the same flags —
the log line `Restored N item(s) from data/server_1/server_1_snapshot.txt`
confirms it picked its state back up.

## Scaling further
- **Add a shard**: pick unused ids/ports and run a new `server`/`proxy`
  pair, e.g. `-id 3 -low 201 -high 300`. It registers itself; nothing
  else needs to change or restart.
- **Scale within a shard**: since a proxy holds no state, run multiple
  proxy instances for the same range behind a load balancer — they all
  register the same range and all forward to the same server. (Note:
  the current registry keeps only the *last* registration per proxy ID,
  so give each instance a distinct id, e.g. `1a`/`1b` via separate ids,
  and put a load balancer in front for clients to hit.)
- **Beyond one server per shard**: if one shard's volume outgrows a
  single server process, that's the next bottleneck — e.g. partition
  further within the range, or move that shard's state into a shared
  store (Redis, Postgres) multiple server replicas can read/write safely.

## Testing the lock
Start `server -id 1 -port 6001 -low 1 -high 100`, then in another terminal
try starting a second one with the same `-id 1` but a different port:
```
$ ./bin/server -id 1 -port 6099 -low 1 -high 100
2026/08/25 06:25:03 [Server 1] shard already running (pid 41823 holds data/server_1/server_1.lock) — refusing to start a duplicate
```
Now `Ctrl+C` the first one (releases the lock cleanly) and start the second
again — it starts normally. Or `kill -9` the first one to simulate a crash,
then start a new one with `-id 1` — you'll see `Reclaimed stale lock
data/server_1/server_1.lock (previous owner not running)` instead of a
refusal.

## Note
As with the earlier versions, I wasn't able to `go build`/`go run` this in
my sandbox (no Go toolchain, no network to install one). I reviewed the
code carefully — checked imports, brace balance, and every renamed
call-site — but please run `go build ./...` on your machine to confirm it
compiles cleanly and flag anything that comes up.