#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestExecuteSQLQuery(ColumnarTopNOffsetPastResult|UsesColumnarTopN|UsesColumnarTopNAfterNumericFilter)$' -count=1
