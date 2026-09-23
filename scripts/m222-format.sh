#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/index_rebuild_replica.go \
  hat/hatSql/materialized.go \
  hat/hatSql/m222_index_rebuild_replica_test.go \
  hat/hatSql/m222_index_rebuild_replica_benchmark_test.go
