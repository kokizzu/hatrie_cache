#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestSQLPreparedQueryCache' -count=1
