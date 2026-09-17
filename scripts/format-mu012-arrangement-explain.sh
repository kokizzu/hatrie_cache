#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/catalog.go \
  hat/hatSql/explain_dataflow.go \
  hat/hatSql/explain_pipeline.go \
  hat/hatSql/materialized.go \
  hat/hatSql/model.go \
  hat/hatSql/mu012_arrangement_explain.go \
  hat/hatSql/mu012_arrangement_explain_benchmark_test.go \
  hat/hatSql/mu012_arrangement_explain_test.go \
  hat/hatSql/query.go \
  hat/hatSql/result_cache.go
