#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/stored_procedure.go \
  hat/hatSql/tu03_stored_procedure_test.go \
  hat/hatSql/tu03_stored_procedure_benchmark_test.go
