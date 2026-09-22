#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/c236_explain_skip_decisions_test.go \
  hat/hatSql/materialized.go \
  hat/hatSql/model.go \
  hat/hatSql/mz050_plan_snapshot.go \
  hat/hatSql/query.go \
  hat/hatSql/result_cache.go
