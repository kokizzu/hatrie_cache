#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestTypedTableAggregateMaintainsExactCountDistinct$' -count=1
go test -race ./hat/hatSql
