#!/usr/bin/env bash
set -euo pipefail

count="${BENCH_COUNT:-5}"
go test ./hat/hatSql -run '^$' -bench 'Benchmark(SQLQueryFingerprint|FormatSQLForFingerprintBaseline)$' -benchmem -count="$count"
