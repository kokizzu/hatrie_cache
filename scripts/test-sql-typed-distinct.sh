#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestTypedTableAggregateMaintainsExactCountDistinct$' -count=1
