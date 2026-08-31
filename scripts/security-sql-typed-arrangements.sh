#!/bin/sh
set -eu

go vet ./...
go test -race ./hat/hatSql -run '^TestTypedTableAggregateArrangement' -count=1
