#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-chu01-idempotency-benchmark.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/gocache" "$tmp_dir/gotmp"
GOCACHE="$tmp_dir/gocache" GOTMPDIR="$tmp_dir/gotmp" go test ./hat/hatCache -run '^$' -bench '^(BenchmarkCommandJournalIdempotencyRetry|BenchmarkCHU01AsyncInsertKeyedDuplicate)$' -benchmem -count=1
