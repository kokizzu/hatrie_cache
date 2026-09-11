#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql ./hat/hatCache -run '^$' -bench '^(BenchmarkSQLOrderedRange|BenchmarkHatTrieSQLOrderedRange)(Stream)?(Baseline|SparseMarks)$' -benchmem -count=5
