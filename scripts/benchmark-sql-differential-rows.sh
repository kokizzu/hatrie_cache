#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkConsolidateDifferentialRows$' -benchmem -count=5
