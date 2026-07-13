#!/usr/bin/env sh
set -eu

addr=${MONITORING_ADDR:-127.0.0.1:8080}
web_dir=${MONITORING_WEB_DIR:-svelte-mpa/dist}

exec go run ./cmd/hatrie-cache \
	-monitoring-server \
	-monitoring-addr "$addr" \
	-monitoring-web-dir "$web_dir"
