#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLIndexedJoinUsesBorrowedIndexPostings$' -count=1
go test ./hat/hatSql -run '^TestSQLIndexedJoinFallsBackWhenBorrowedPostingsUnavailable$' -count=1
go test ./hat/hatCache -run '^(TestBorrowSQLIndexedSourceRefreshesImmutablePostings|TestSQLBorrowedIndexJoinKeepsHashPlanForHotDimension)$' -count=1
