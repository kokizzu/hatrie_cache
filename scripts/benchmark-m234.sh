#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m234
go test -run '^$' -bench '^BenchmarkM234' -benchtime=200ms -count=5 ./hat/hatPipeline > M234_BENCHMARK_RAW.txt
cat M234_BENCHMARK_RAW.txt
