#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m233
go test -run '^$' -bench '^BenchmarkM233' -benchtime=200ms -count=5 ./hat/hatSql > M233_BENCHMARK_RAW.txt
cat M233_BENCHMARK_RAW.txt
