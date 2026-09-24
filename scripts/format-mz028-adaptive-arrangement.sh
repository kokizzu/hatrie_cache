#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/typed_table_aggregate_dictionary.go \
  hat/hatSql/mz028_adaptive_arrangement_test.go \
  hat/hatSql/mz028_batched_merge_test.go
