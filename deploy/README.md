# Deployment Examples

These files are starting points for operators. Edit paths, addresses, users,
resource limits, and network controls before using them outside a local test
environment.

- `systemd/hatrie-cache.service` runs one durable node from an installed binary.
- `hatrie-cache.json` is a checked daemon config used by `make verify-local`. It
  uses localhost binding, audited/write-protected admin operations, binary
  LevelDB/journal records, protobuf replication payloads, periodic LevelDB sync
  and compaction, hot-load for cold starts, and a durable async replication
  outbox.
- `topology/full-replica.json` defines two nodes that both own every key.
- `topology/sharded.json` defines two shards over 1024 virtual buckets.
- `docker-compose.yml` runs two local nodes from the checked-out source tree.
- `docker-compose.production.yml` runs one hardened container from a built image
  with durable volumes, API token wiring, and the image healthcheck.

For production, prefer a compiled binary and a private network or explicit API
authentication in front of the monitoring/API port.
