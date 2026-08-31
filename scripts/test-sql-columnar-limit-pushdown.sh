#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestSQLColumnarStreamMaterialize(StopsAfterLimit|WithScanRetainsAllMatches)$' -count=1
