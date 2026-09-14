#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTR019ColumnarValue(Baseline)?$' -benchmem -count=5
