#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLSetOperationAll/(IntersectDistinct|IntersectAll|ExceptAll)$' -benchmem -count=5
