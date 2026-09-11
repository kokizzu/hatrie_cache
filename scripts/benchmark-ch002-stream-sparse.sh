#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLOrderedRangeStreamSparseMarks$' -benchmem -count=5
