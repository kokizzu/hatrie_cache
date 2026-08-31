#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarDictionaryGroupAggregateUsesDictionaryLiteralIN$' -count=1
