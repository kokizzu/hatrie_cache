#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLTableSampleRowsBaseline$' -benchmem -count=5
