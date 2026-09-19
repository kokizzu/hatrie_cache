#!/bin/sh
set -eu

go test ./hat/hatSql/incremental_window_contract_test -run '^$' -bench '^BenchmarkIncrementalRowNumberLagWindowApply$' -benchmem -count=5 -benchtime=100ms
