#!/usr/bin/env sh
set -eu

printf '%s\n' 'running CH-G03 partial aggregate state tests'
go test -count=1 -run 'TestSQLPartialAggregateState' ./hat/hatSql
