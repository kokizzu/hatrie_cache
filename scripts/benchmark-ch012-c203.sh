#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkCH012DeleteMaskBacking|BenchmarkTypedTable(DeleteReinsert|RowsAfterHalfDeletes|PatchCompaction))$' -benchmem -count=5
