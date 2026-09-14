#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTable(DeleteReinsert|RowsAfterHalfDeletes)$' -benchmem -count=5
