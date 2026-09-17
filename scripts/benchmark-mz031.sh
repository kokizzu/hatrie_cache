#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ031RankedTopK(RebuildChangeBaseline|Incremental)$' -benchmem -benchtime=1000x -count=5 -cpu=1
