#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarNumericAggregateUsesNumericVectorConjunction$' -count=1
