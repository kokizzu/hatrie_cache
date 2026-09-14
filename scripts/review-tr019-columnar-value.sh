#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test ./hat/hatSql ./hat/hatCache -count=1
go test -race ./hat/hatSql -run '^TestTR019ColumnarValuePreservesPhysicalPrecedence$' -count=1
go vet ./hat/hatSql ./hat/hatCache
rg -n 'C168|TR-019 Plain Columnar Value Fast Path|TR-019a|benchmark-tr019-columnar-value' INSPIRATION.md INSPIRATION_BACKLOG.md TR019_COLUMNAR_VALUE_FASTPATH.md BENCHMARK.md
