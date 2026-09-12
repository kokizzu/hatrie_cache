#!/bin/sh
set -eu
exec gofmt -w hat/hatReplication/connection_pool.go hat/hatReplication/connection_pool_test.go hat/hatReplication/connection_pool_benchmark_test.go
