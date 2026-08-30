#!/bin/sh
set -eu

go test ./hat/hatSql ./hat/hatCache -run '^(TestSQLColumnarSourceResolverUsesBorrowedImmutableBatchWhenAvailable|TestHatTrieBorrowSQLColumnarSourceSharesOnlyImmutableLayout)$' -count=1
