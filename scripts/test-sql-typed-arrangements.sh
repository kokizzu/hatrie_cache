#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestTypedTableAggregateArrangement' -count=1
