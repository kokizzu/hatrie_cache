#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestExecuteSQLQuery(ColumnarTopNOffsetPastResult|UsesColumnarTopN|UsesColumnarTopNAfter(Numeric|Dictionary)Filter)$' -count=1
