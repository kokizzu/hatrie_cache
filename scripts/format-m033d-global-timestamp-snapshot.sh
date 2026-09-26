#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/global_timestamp_oracle_snapshot_store.go \
  hat/hatReplication/m033d_global_timestamp_snapshot_test.go
