#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
iterations="${BENCH_ITERS:-20}"
go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournalIdempotency' -benchtime="${iterations}x" -count=5
