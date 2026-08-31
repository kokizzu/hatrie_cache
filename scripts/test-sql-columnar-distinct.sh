#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestExecuteSQLQueryUsesColumnarDictionaryDistinct' -count=1
