#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/mz009_validity_partition_pruning_test.go
