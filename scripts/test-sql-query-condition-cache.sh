#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLQueryConditionCache' -count=1
