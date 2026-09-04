#!/bin/sh
set -eu

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkDelayQueuePushPop$' -benchmem -count="${BENCHCOUNT:-5}" -benchtime="${BENCHTIME:-1s}"
