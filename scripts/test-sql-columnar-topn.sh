#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestExecuteSQLQuery(ColumnarTopNOffsetPastResult|UsesColumnarTopN|UsesNumericSegmentTopNPruningWithoutSkippingTies)$' -count=1
go test ./hat/hatCache -run '^TestHatTrieSQLColumnarTopNNumericSegmentPruning$' -count=1
