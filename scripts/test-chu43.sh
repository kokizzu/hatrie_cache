#!/bin/sh
set -eu
go test ./hat/hatSql -run 'TestSQLTDigestAggregateState' -count=1
