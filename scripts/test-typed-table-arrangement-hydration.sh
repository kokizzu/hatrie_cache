#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestTypedTableAggregateArrangementHydrates' -count=1
