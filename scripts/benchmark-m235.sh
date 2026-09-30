#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m235
go test -run '^$' -bench '^BenchmarkM235' -benchtime=200ms -count=5 ./hat/hatSql > M235_BENCHMARK_RAW.txt
cat M235_BENCHMARK_RAW.txt
