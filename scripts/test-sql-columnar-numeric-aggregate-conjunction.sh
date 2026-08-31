#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarNumericAggregateUses(NumericVectorConjunction|DictionaryNumericConjunction)$' -count=1
