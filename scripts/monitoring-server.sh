#!/usr/bin/env sh
set -eu

addr=${MONITORING_ADDR:-127.0.0.1:8080}
web_dir=${MONITORING_WEB_DIR:-svelte-mpa/dist}
snapshot_path=${SNAPSHOT_PATH:-}
snapshot_interval=${SNAPSHOT_INTERVAL:-0}

exec go run ./cmd/hatrie-cache \
	-monitoring-server \
	-monitoring-addr "$addr" \
	-monitoring-web-dir "$web_dir" \
	-snapshot-path "$snapshot_path" \
	-snapshot-interval "$snapshot_interval"
