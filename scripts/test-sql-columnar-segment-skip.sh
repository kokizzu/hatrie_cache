#!/bin/sh
set -eu

go test ./hat/hatSql ./hat/hatCache -run '^(TestSQLColumnarNumericAggregateUsesSegmentedBatchWhenAvailable|TestSQLColumnarDictionarySegmentMasksPreservePredicateSemantics|TestHatTrieBorrowSQLColumnarSourceSegmentsPromotesAndInvalidates|TestHatTrieSQLColumnarDictionarySegmentSkipMatchesMaterializedQuery)$' -count=1
