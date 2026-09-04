#!/bin/sh
set -eu

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkDeadLetterQueueReplay$' -benchmem -count=${BENCHCOUNT:-5} -benchtime=${BENCHTIME:-1s}
