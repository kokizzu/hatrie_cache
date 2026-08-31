#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestExecuteSQLQuery(ColumnarTopNOffsetPastResult|UsesColumnarTopN)$' -count=1
