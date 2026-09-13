#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLBoundedGroupArray/(unbounded|filtered-unbounded)$' -benchmem -count=5
