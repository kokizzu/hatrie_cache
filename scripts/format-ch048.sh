#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/external_schema.go \
  hat/hatSql/ch048_external_schema_inference_test.go \
  hat/hatSql/ch048_external_schema_inference_benchmark_test.go
