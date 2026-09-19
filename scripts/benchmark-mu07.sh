#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialDataflowApply$' -benchmem -count=5 -benchtime=100ms
