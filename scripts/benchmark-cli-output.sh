#!/bin/sh
set -eu

go test ./cmd/hatrie-cli -run '^$' -bench '^BenchmarkCLIOutputWriter$' -benchmem -count=${BENCHCOUNT:-5} -benchtime=${BENCHTIME:-1s}
