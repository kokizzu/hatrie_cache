#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/contracts.go \
  hat/hatSql/external.go \
  hat/hatSql/query.go \
  hat/hatSql/chu02_external_order_spill_test.go \
  hat/hatSql/chu02_external_order_spill_benchmark_test.go
