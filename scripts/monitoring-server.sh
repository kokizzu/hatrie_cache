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
key_stats_mode=${KEY_STATS_MODE:-off}
key_stats_capacity=${KEY_STATS_CAPACITY:-100000}
read_header_timeout=${MONITORING_READ_HEADER_TIMEOUT:-5s}
idle_timeout=${MONITORING_IDLE_TIMEOUT:-2m}
node_id=${NODE_ID:-}
topology_path=${TOPOLOGY_PATH:-}
election_timeout=${ELECTION_TIMEOUT:-15s}
replication=${REPLICATION:-false}
replication_mode=${REPLICATION_MODE:-journal}
replication_async=${REPLICATION_ASYNC:-false}
replication_queue_size=${REPLICATION_QUEUE_SIZE:-1024}
replication_retry_interval=${REPLICATION_RETRY_INTERVAL:-250ms}
replication_max_attempts=${REPLICATION_MAX_ATTEMPTS:-3}
replication_dead_letter_limit=${REPLICATION_DEAD_LETTER_LIMIT:-128}
replication_outbox_path=${REPLICATION_OUTBOX_PATH:-}
replication_outbox_format=${REPLICATION_OUTBOX_FORMAT:-auto}
replication_outbox_codec=${REPLICATION_OUTBOX_CODEC:-binary}
replication_outbox_batch_window=${REPLICATION_OUTBOX_BATCH_WINDOW:-1ms}
replication_circuit_breaker_failures=${REPLICATION_CIRCUIT_BREAKER_FAILURES:-5}
replication_circuit_breaker_cooldown=${REPLICATION_CIRCUIT_BREAKER_COOLDOWN:-30s}
replication_wire_format=${REPLICATION_WIRE_FORMAT:-protobuf}
replication_transport=${REPLICATION_TRANSPORT:-http}
replication_grpc_window=${REPLICATION_GRPC_WINDOW:-32}
replication_http_fallback=${REPLICATION_HTTP_FALLBACK:-true}
replication_auth_token=${REPLICATION_AUTH_TOKEN:-}
replication_batch_max_bytes=${REPLICATION_BATCH_MAX_BYTES:-1048576}
replication_max_in_flight_targets=${REPLICATION_MAX_IN_FLIGHT_TARGETS:-4}
replication_sync_interval=${REPLICATION_SYNC_INTERVAL:-0}
replication_sync_prefix=${REPLICATION_SYNC_PREFIX:-}
enforce_leader_writes=${ENFORCE_LEADER_WRITES:-false}
grpc_addr=${GRPC_ADDR:-}
grpc_tls_cert=${GRPC_TLS_CERT:-}
grpc_tls_key=${GRPC_TLS_KEY:-}
grpc_client_ca=${GRPC_CLIENT_CA:-}
db_path=${DB_PATH:-}
db_format=${DB_FORMAT:-binary}
db_sync_interval=${DB_SYNC_INTERVAL:-0}
db_compare_before_write=${DB_COMPARE_BEFORE_WRITE:-auto}
db_compact_interval=${DB_COMPACT_INTERVAL:-0}
db_compact_start_key=${DB_COMPACT_START_KEY:-}
db_compact_limit_key=${DB_COMPACT_LIMIT_KEY:-}
db_hot_load=${DB_HOT_LOAD:-false}
db_hot_load_max_bytes=${DB_HOT_LOAD_MAX_BYTES:-1024}
db_hot_load_max_age=${DB_HOT_LOAD_MAX_AGE:-1h}
db_hot_load_min_hits=${DB_HOT_LOAD_MIN_HITS:-1000}
db_memory_cap_bytes=${DB_MEMORY_CAP_BYTES:-0}
db_rss_cap_bytes=${DB_RSS_CAP_BYTES:-0}
db_memory_evict_interval=${DB_MEMORY_EVICT_INTERVAL:-0}
db_memory_evict_min_value_bytes=${DB_MEMORY_EVICT_MIN_VALUE_BYTES:-1024}
snapshot_path=${SNAPSHOT_PATH:-}
snapshot_interval=${SNAPSHOT_INTERVAL:-0}
snapshot_format=${SNAPSHOT_FORMAT:-gzip-best-binary}
journal_path=${JOURNAL_PATH:-}
journal_format=${JOURNAL_FORMAT:-binary}
journal_group_commit_window=${JOURNAL_GROUP_COMMIT_WINDOW:-0}
journal_group_commit_max_batch=${JOURNAL_GROUP_COMMIT_MAX_BATCH:-64}
journal_pull_source=${JOURNAL_PULL_SOURCE:-}
journal_pull_state_path=${JOURNAL_PULL_STATE_PATH:-}
journal_pull_interval=${JOURNAL_PULL_INTERVAL:-0}
journal_pull_timeout=${JOURNAL_PULL_TIMEOUT:-30s}
journal_pull_limit=${JOURNAL_PULL_LIMIT:-0}
journal_pull_max_batches=${JOURNAL_PULL_MAX_BATCHES:-0}
journal_pull_full_sync_fallback=${JOURNAL_PULL_FULL_SYNC_FALLBACK:-true}

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
	-key-stats-mode "$key_stats_mode" \
	-key-stats-capacity "$key_stats_capacity" \
	-monitoring-read-header-timeout "$read_header_timeout" \
	-monitoring-idle-timeout "$idle_timeout" \
	-node-id "$node_id" \
	-topology-path "$topology_path" \
	-election-timeout "$election_timeout" \
	-replication="$replication" \
	-replication-mode "$replication_mode" \
	-replication-async="$replication_async" \
	-replication-queue-size "$replication_queue_size" \
	-replication-retry-interval "$replication_retry_interval" \
	-replication-max-attempts "$replication_max_attempts" \
	-replication-dead-letter-limit "$replication_dead_letter_limit" \
	-replication-outbox-path "$replication_outbox_path" \
	-replication-outbox-format "$replication_outbox_format" \
	-replication-outbox-codec "$replication_outbox_codec" \
	-replication-outbox-batch-window "$replication_outbox_batch_window" \
	-replication-circuit-breaker-failures "$replication_circuit_breaker_failures" \
	-replication-circuit-breaker-cooldown "$replication_circuit_breaker_cooldown" \
	-replication-auth-token "$replication_auth_token" \
	-replication-transport "$replication_transport" \
	-replication-grpc-window "$replication_grpc_window" \
	-replication-http-fallback="$replication_http_fallback" \
	-replication-batch-max-bytes "$replication_batch_max_bytes" \
	-replication-max-in-flight-targets "$replication_max_in_flight_targets" \
	-replication-sync-interval "$replication_sync_interval" \
	-replication-sync-prefix "$replication_sync_prefix" \
	-enforce-leader-writes="$enforce_leader_writes" \
	-grpc-addr "$grpc_addr" \
	-grpc-tls-cert "$grpc_tls_cert" \
	-grpc-tls-key "$grpc_tls_key" \
	-grpc-client-ca "$grpc_client_ca" \
	-db-path "$db_path" \
	-db-sync-interval "$db_sync_interval" \
	-db-compare-before-write "$db_compare_before_write" \
	-db-compact-interval "$db_compact_interval" \
	-db-compact-start-key "$db_compact_start_key" \
	-db-compact-limit-key "$db_compact_limit_key" \
	-db-hot-load="$db_hot_load" \
	-db-hot-load-max-bytes "$db_hot_load_max_bytes" \
	-db-hot-load-max-age "$db_hot_load_max_age" \
	-db-hot-load-min-hits "$db_hot_load_min_hits" \
	-db-memory-cap-bytes "$db_memory_cap_bytes" \
	-db-rss-cap-bytes "$db_rss_cap_bytes" \
	-db-memory-evict-interval "$db_memory_evict_interval" \
	-db-memory-evict-min-value-bytes "$db_memory_evict_min_value_bytes" \
	-snapshot-path "$snapshot_path" \
	-snapshot-interval "$snapshot_interval" \
	-journal-path "$journal_path" \
	-journal-group-commit-window "$journal_group_commit_window" \
	-journal-group-commit-max-batch "$journal_group_commit_max_batch" \
	-journal-pull-source "$journal_pull_source" \
	-journal-pull-state-path "$journal_pull_state_path" \
	-journal-pull-interval "$journal_pull_interval" \
	-journal-pull-timeout "$journal_pull_timeout" \
	-journal-pull-limit "$journal_pull_limit" \
	-journal-pull-max-batches "$journal_pull_max_batches" \
	-journal-pull-full-sync-fallback="$journal_pull_full_sync_fallback"

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
