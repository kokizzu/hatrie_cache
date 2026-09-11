#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLGroupingSetIdentifiers$' -benchmem -count=10
