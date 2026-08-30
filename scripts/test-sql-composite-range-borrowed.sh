#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLCompositeRangePlannerBorrowsRowsWithoutChangingPublicCopies$' -count=1
