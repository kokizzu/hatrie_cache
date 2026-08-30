#!/bin/sh
set -eu

go test ./hat/hatSql ./hat/hatCache -run '^(TestSQLColumnarNumericAggregateUsesSegmentedBatchWhenAvailable|TestHatTrieBorrowSQLColumnarSourceSegmentsPromotesAndInvalidates)$' -count=1
