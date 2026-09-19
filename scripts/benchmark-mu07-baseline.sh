#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkApplyDifferentialLateDataPolicy|BenchmarkDifferentialWatermarkApply)$' -benchmem -count=5 -benchtime=100ms
