#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC211JoinOrderBaseline$' -benchmem -count=5
