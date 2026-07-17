#!/usr/bin/env sh
set -eu

tarantool_bin=${TARANTOOL_BIN:-tarantool}
requests=${TARANTOOL_REQUESTS:-10000}
keyspace=${TARANTOOL_KEYSPACE:-10000}
memtx_memory=${TARANTOOL_MEMTX_MEMORY:-268435456}
work_dir=${TARANTOOL_WORK_DIR:-}
artifact_dir=${BENCHMARK_ARTIFACT_DIR:-${TARANTOOL_BENCHMARK_ARTIFACT_DIR:-}}
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

if [ -n "$artifact_dir" ]; then
	mkdir -p "$artifact_dir"
	raw_file="$artifact_dir/tarantool-command-features.md"
	rows_tsv="$artifact_dir/tarantool-command-features.tsv"
	memory_tsv="$artifact_dir/tarantool-command-memory.tsv"
	TARANTOOL_REQUESTS="$requests" \
	TARANTOOL_KEYSPACE="$keyspace" \
	TARANTOOL_MEMTX_MEMORY="$memtx_memory" \
	TARANTOOL_WORK_DIR="$work_dir" \
	"$tarantool_bin" scripts/tarantool-command-features.lua >"$raw_file"
	cat "$raw_file"
	awk -F'|' '
		function trim(value) {
			gsub(/^[ \t]+|[ \t]+$/, "", value)
			return value
		}
		BEGIN {
			print "feature\toperation\tseconds_per_10k"
		}
		/^\|/ && $2 !~ /Feature family|---/ && $4 ~ / s / {
			feature = trim($2)
			operation = trim($3)
			seconds = trim($4)
			gsub(/`/, "", operation)
			gsub(/ s$/, "", seconds)
			print feature "\t" operation "\t" seconds
		}
	' "$raw_file" >"$rows_tsv"
	awk -F'|' '
		function trim(value) {
			gsub(/^[ \t]+|[ \t]+$/, "", value)
			return value
		}
		BEGIN {
			print "metric\tvalue\tunit"
		}
		/^Memory summary:/ {
			in_memory = 1
			next
		}
		in_memory && /^\|/ && $2 !~ /Metric|---/ && $3 !~ /^ *$/ {
			metric = trim($2)
			value = trim($3)
			split(value, parts, " ")
			if (length(parts[2]) == 0) {
				parts[2] = ""
			}
			print metric "\t" parts[1] "\t" parts[2]
		}
	' "$raw_file" >"$memory_tsv"
else
	TARANTOOL_REQUESTS="$requests" \
	TARANTOOL_KEYSPACE="$keyspace" \
	TARANTOOL_MEMTX_MEMORY="$memtx_memory" \
	TARANTOOL_WORK_DIR="$work_dir" \
	"$tarantool_bin" scripts/tarantool-command-features.lua
fi
