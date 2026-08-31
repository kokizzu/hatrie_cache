#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarNumericAggregateUsesDictionaryLiteralIN$' -count=1
