#!/bin/sh
set -eu
GOMAXPROCS=1 go test ./hat/hatCache -run '^$' -bench '^BenchmarkCHU23' -benchmem -cpu=1 -count=5
