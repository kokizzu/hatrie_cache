#!/bin/sh
set -eu

go test -race ./hat/hatSql -run '^TestTypedTableAggregateArrangement' -count=1
