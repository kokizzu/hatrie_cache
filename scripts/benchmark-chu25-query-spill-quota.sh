#!/bin/sh
set -eu

GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU25QuerySpillQuota$' -benchmem -count=5 -cpu=1
