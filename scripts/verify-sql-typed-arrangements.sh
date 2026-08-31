#!/bin/sh
set -eu

go test ./hat/hatSql -run '^(TestTypedTable|TestTypedTableAggregateArrangements)' -count=1
go vet ./hat/hatSql
