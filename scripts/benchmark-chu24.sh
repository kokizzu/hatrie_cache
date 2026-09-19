#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkCH010PlainUpsert|BenchmarkCHU24TypedTableUpsert)' -benchmem -count=5
