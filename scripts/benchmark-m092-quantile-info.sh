#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLApproximatePercentile(Scalar|Info)$' -benchmem -count=5
