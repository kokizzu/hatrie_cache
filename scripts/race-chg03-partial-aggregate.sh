#!/usr/bin/env sh
set -eu

printf '%s\n' 'running CH-G03 partial aggregate state race tests'
go test -race -count=1 -run 'TestSQLPartialAggregateState' ./hat/hatSql
