#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkCHG42SQLQueryMemoryTracking|BenchmarkCH230MemoryOvercommit)$' -benchmem -count=5
