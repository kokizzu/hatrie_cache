#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTR019ColumnarBatch(Value|PrepareFieldOffsets)' -benchmem -count=5
