#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m233
gofmt -w hat/hatSql/m233_sink_retry_dedup.go hat/hatSql/m233_sink_retry_dedup_test.go hat/hatSql/m233_sink_retry_dedup_benchmark_test.go
