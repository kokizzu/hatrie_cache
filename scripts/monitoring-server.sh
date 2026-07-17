#!/usr/bin/env sh
set -eu

addr=${MONITORING_ADDR:-127.0.0.1:8080}
web_dir=${MONITORING_WEB_DIR:-svelte-mpa/dist}
tls_cert=${MONITORING_TLS_CERT:-}
tls_key=${MONITORING_TLS_KEY:-}
auth_token=${MONITORING_AUTH_TOKEN:-}
audit_log_path=${AUDIT_LOG_PATH:-}
write_protection=${WRITE_PROTECTION:-false}
rate_limit=${RATE_LIMIT:-0}
read_header_timeout=${MONITORING_READ_HEADER_TIMEOUT:-5s}
idle_timeout=${MONITORING_IDLE_TIMEOUT:-2m}
node_id=${NODE_ID:-}
topology_path=${TOPOLOGY_PATH:-}
election_timeout=${ELECTION_TIMEOUT:-15s}
replication=${REPLICATION:-false}
replication_async=${REPLICATION_ASYNC:-false}
replication_queue_size=${REPLICATION_QUEUE_SIZE:-1024}
replication_retry_interval=${REPLICATION_RETRY_INTERVAL:-250ms}
replication_max_attempts=${REPLICATION_MAX_ATTEMPTS:-3}
replication_dead_letter_limit=${REPLICATION_DEAD_LETTER_LIMIT:-128}
replication_circuit_breaker_failures=${REPLICATION_CIRCUIT_BREAKER_FAILURES:-5}
replication_circuit_breaker_cooldown=${REPLICATION_CIRCUIT_BREAKER_COOLDOWN:-30s}
replication_wire_format=${REPLICATION_WIRE_FORMAT:-}
replication_sync_interval=${REPLICATION_SYNC_INTERVAL:-0}
replication_sync_prefix=${REPLICATION_SYNC_PREFIX:-}
enforce_leader_writes=${ENFORCE_LEADER_WRITES:-false}
grpc_addr=${GRPC_ADDR:-}
grpc_tls_cert=${GRPC_TLS_CERT:-}
grpc_tls_key=${GRPC_TLS_KEY:-}
grpc_client_ca=${GRPC_CLIENT_CA:-}
db_path=${DB_PATH:-}
db_format=${DB_FORMAT:-}
db_sync_interval=${DB_SYNC_INTERVAL:-0}
db_hot_load=${DB_HOT_LOAD:-false}
db_hot_load_max_bytes=${DB_HOT_LOAD_MAX_BYTES:-1024}
db_hot_load_max_age=${DB_HOT_LOAD_MAX_AGE:-1h}
db_hot_load_min_hits=${DB_HOT_LOAD_MIN_HITS:-1000}
snapshot_path=${SNAPSHOT_PATH:-}
snapshot_interval=${SNAPSHOT_INTERVAL:-0}
snapshot_format=${SNAPSHOT_FORMAT:-}
journal_path=${JOURNAL_PATH:-}
journal_format=${JOURNAL_FORMAT:-}
journal_pull_source=${JOURNAL_PULL_SOURCE:-}
journal_pull_state_path=${JOURNAL_PULL_STATE_PATH:-}
journal_pull_interval=${JOURNAL_PULL_INTERVAL:-0}
journal_pull_timeout=${JOURNAL_PULL_TIMEOUT:-30s}
journal_pull_limit=${JOURNAL_PULL_LIMIT:-0}
journal_pull_max_batches=${JOURNAL_PULL_MAX_BATCHES:-0}

set -- \
	-monitoring-server \
	-monitoring-addr "$addr" \
	-monitoring-web-dir "$web_dir" \
	-monitoring-tls-cert "$tls_cert" \
	-monitoring-tls-key "$tls_key" \
	-monitoring-auth-token "$auth_token" \
	-audit-log-path "$audit_log_path" \
	-write-protection="$write_protection" \
	-rate-limit "$rate_limit" \
	-monitoring-read-header-timeout "$read_header_timeout" \
	-monitoring-idle-timeout "$idle_timeout" \
	-node-id "$node_id" \
	-topology-path "$topology_path" \
	-election-timeout "$election_timeout" \
	-replication="$replication" \
	-replication-async="$replication_async" \
	-replication-queue-size "$replication_queue_size" \
	-replication-retry-interval "$replication_retry_interval" \
	-replication-max-attempts "$replication_max_attempts" \
	-replication-dead-letter-limit "$replication_dead_letter_limit" \
	-replication-circuit-breaker-failures "$replication_circuit_breaker_failures" \
	-replication-circuit-breaker-cooldown "$replication_circuit_breaker_cooldown" \
	-replication-sync-interval "$replication_sync_interval" \
	-replication-sync-prefix "$replication_sync_prefix" \
	-enforce-leader-writes="$enforce_leader_writes" \
	-grpc-addr "$grpc_addr" \
	-grpc-tls-cert "$grpc_tls_cert" \
	-grpc-tls-key "$grpc_tls_key" \
	-grpc-client-ca "$grpc_client_ca" \
	-db-path "$db_path" \
	-db-sync-interval "$db_sync_interval" \
	-db-hot-load="$db_hot_load" \
	-db-hot-load-max-bytes "$db_hot_load_max_bytes" \
	-db-hot-load-max-age "$db_hot_load_max_age" \
	-db-hot-load-min-hits "$db_hot_load_min_hits" \
	-snapshot-path "$snapshot_path" \
	-snapshot-interval "$snapshot_interval" \
	-journal-path "$journal_path" \
	-journal-pull-source "$journal_pull_source" \
	-journal-pull-state-path "$journal_pull_state_path" \
	-journal-pull-interval "$journal_pull_interval" \
	-journal-pull-timeout "$journal_pull_timeout" \
	-journal-pull-limit "$journal_pull_limit" \
	-journal-pull-max-batches "$journal_pull_max_batches"

if [ -n "$replication_wire_format" ]; then
	set -- "$@" -replication-wire-format "$replication_wire_format"
fi
if [ -n "$db_format" ]; then
	set -- "$@" -db-format "$db_format"
fi
if [ -n "$snapshot_format" ]; then
	set -- "$@" -snapshot-format "$snapshot_format"
fi
if [ -n "$journal_format" ]; then
	set -- "$@" -journal-format "$journal_format"
fi

exec go run ./cmd/hatrie-cache "$@"
