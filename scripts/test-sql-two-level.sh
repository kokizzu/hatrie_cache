#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLColumnarTwoLevelGroupAggregateMatchesSequentialResults$' -count=1
