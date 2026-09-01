#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestTemporalTableAsOfOrdersOutOfOrderAndReturnsIndependentRows$' -count=1
