#!/usr/bin/env sh
set -eu

addr=${MONITORING_ADDR:-127.0.0.1:8080}
web_dir=${MONITORING_WEB_DIR:-svelte-mpa/dist}
tls_cert=${MONITORING_TLS_CERT:-}
tls_key=${MONITORING_TLS_KEY:-}
node_id=${NODE_ID:-}
topology_path=${TOPOLOGY_PATH:-}
election_timeout=${ELECTION_TIMEOUT:-15s}
replication=${REPLICATION:-false}
grpc_addr=${GRPC_ADDR:-}
db_path=${DB_PATH:-}
db_sync_interval=${DB_SYNC_INTERVAL:-0}
db_hot_load=${DB_HOT_LOAD:-false}
db_hot_load_max_bytes=${DB_HOT_LOAD_MAX_BYTES:-1024}
db_hot_load_max_age=${DB_HOT_LOAD_MAX_AGE:-1h}
db_hot_load_min_hits=${DB_HOT_LOAD_MIN_HITS:-1000}
snapshot_path=${SNAPSHOT_PATH:-}
snapshot_interval=${SNAPSHOT_INTERVAL:-0}
journal_path=${JOURNAL_PATH:-}

exec go run ./cmd/hatrie-cache \
	-monitoring-server \
	-monitoring-addr "$addr" \
	-monitoring-web-dir "$web_dir" \
	-monitoring-tls-cert "$tls_cert" \
	-monitoring-tls-key "$tls_key" \
	-node-id "$node_id" \
	-topology-path "$topology_path" \
	-election-timeout "$election_timeout" \
	-replication="$replication" \
	-grpc-addr "$grpc_addr" \
	-db-path "$db_path" \
	-db-sync-interval "$db_sync_interval" \
	-db-hot-load="$db_hot_load" \
	-db-hot-load-max-bytes "$db_hot_load_max_bytes" \
	-db-hot-load-max-age "$db_hot_load_max_age" \
	-db-hot-load-min-hits "$db_hot_load_min_hits" \
	-snapshot-path "$snapshot_path" \
	-snapshot-interval "$snapshot_interval" \
	-journal-path "$journal_path"
