#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLColumnarDictionaryGroupAggregate$' -count=1
