#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLAsOf(LiveBaseline|Historical)$' -benchmem -count=5
