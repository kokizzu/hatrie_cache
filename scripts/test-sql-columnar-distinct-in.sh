#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarDictionaryDistinctUsesDictionaryLiteralIN$' -count=1
