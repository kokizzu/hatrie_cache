#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestSQLQueryConditionCache' -count=1
