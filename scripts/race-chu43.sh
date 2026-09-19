#!/bin/sh
set -eu
go test ./hat/hatSql -run 'TestSQLTDigestAggregateState' -race -count=1
