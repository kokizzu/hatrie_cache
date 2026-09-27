#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatReplication/global_timestamp_lease_pool.go \
    hat/hatReplication/global_timestamp_lease_pool_test.go \
    hat/hatReplication/global_timestamp_lease_pool_benchmark_test.go
