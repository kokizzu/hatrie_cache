#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/global_timestamp_oracle.go \
  hat/hatReplication/global_timestamp_oracle_test.go \
  hat/hatReplication/global_timestamp_oracle_benchmark_test.go \
  hat/hatReplication/global_timestamp_oracle_file_store.go \
  hat/hatReplication/global_timestamp_oracle_file_store_test.go \
  hat/hatReplication/global_timestamp_oracle_file_store_benchmark_test.go
