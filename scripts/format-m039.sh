#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/partition_order.go \
  hat/hatSql/m_u39_partition_order_test.go \
  hat/hatSql/model.go \
  hat/hatSql/query.go \
  hat/hatSql/explain_pipeline.go
