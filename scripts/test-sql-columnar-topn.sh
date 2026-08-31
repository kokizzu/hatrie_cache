#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestExecuteSQLQuery(ColumnarTopNOffsetPastResult|UsesColumnarTopN|UsesColumnarTopNAfter(Numeric|Dictionary)Filter|UsesColumnarTopNForDictionaryOrder)$' -count=1
