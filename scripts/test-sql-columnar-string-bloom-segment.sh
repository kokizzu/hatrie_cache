#!/bin/sh
set -eu

go test ./hat/hatSql ./hat/hatCache -run '^(TestColumnarStringBloomSegmentHasNoFalseNegatives|TestSQLColumnarStringEqualityPredicateRequiresBinaryCollation|TestHatTrieSQLColumnarStringBloomSegmentSkipMatchesMaterializedQuery)$' -count=1
