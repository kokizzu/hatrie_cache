#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/c237_estimated_io_cost_test.go \
  hat/hatSql/explain_dataflow.go \
  hat/hatSql/explain_pipeline.go \
  hat/hatSql/model.go \
  hat/hatSql/mz044_costed_explain.go \
  hat/hatSql/mz044_costed_explain_test.go \
  hat/hatSql/query.go \
  hat/hatSql/result_cache.go
