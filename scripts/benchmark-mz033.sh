#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ033ManualDataflowRecommendation$' -benchmem -count=5 -benchtime=3s
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ033SQLDataflowIndexRecommendation$' -benchmem -count=5 -benchtime=3s
