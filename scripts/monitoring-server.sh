#!/usr/bin/env sh
set -eu

addr=${MONITORING_ADDR:-127.0.0.1:8080}
web_dir=${MONITORING_WEB_DIR:-svelte-mpa/dist}
tls_cert=${MONITORING_TLS_CERT:-}
tls_key=${MONITORING_TLS_KEY:-}
grpc_addr=${GRPC_ADDR:-}
db_path=${DB_PATH:-}
db_sync_interval=${DB_SYNC_INTERVAL:-0}
snapshot_path=${SNAPSHOT_PATH:-}
snapshot_interval=${SNAPSHOT_INTERVAL:-0}
journal_path=${JOURNAL_PATH:-}

exec go run ./cmd/hatrie-cache \
	-monitoring-server \
	-monitoring-addr "$addr" \
	-monitoring-web-dir "$web_dir" \
	-monitoring-tls-cert "$tls_cert" \
	-monitoring-tls-key "$tls_key" \
	-grpc-addr "$grpc_addr" \
	-db-path "$db_path" \
	-db-sync-interval "$db_sync_interval" \
	-snapshot-path "$snapshot_path" \
	-snapshot-interval "$snapshot_interval" \
	-journal-path "$journal_path"
