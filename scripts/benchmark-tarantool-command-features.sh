#!/usr/bin/env sh
set -eu

tarantool_bin=${TARANTOOL_BIN:-tarantool}
requests=${TARANTOOL_REQUESTS:-10000}
keyspace=${TARANTOOL_KEYSPACE:-10000}
memtx_memory=${TARANTOOL_MEMTX_MEMORY:-268435456}
work_dir=${TARANTOOL_WORK_DIR:-}
created_work_dir=

if [ -z "$work_dir" ]; then
	work_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tarantool-bench.XXXXXX")
	created_work_dir=1
else
	mkdir -p "$work_dir"
fi

cleanup() {
	if [ -n "$created_work_dir" ]; then
		rm -rf "$work_dir"
	fi
}
trap cleanup EXIT HUP INT TERM

TARANTOOL_REQUESTS="$requests" \
TARANTOOL_KEYSPACE="$keyspace" \
TARANTOOL_MEMTX_MEMORY="$memtx_memory" \
TARANTOOL_WORK_DIR="$work_dir" \
"$tarantool_bin" scripts/tarantool-command-features.lua
