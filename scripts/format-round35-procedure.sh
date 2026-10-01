#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatProcedure/doc.go \
  hat/hatProcedure/procedure.go \
  hat/hatProcedure/procedure_test.go \
  hat/hatProcedure/procedure_benchmark_test.go
