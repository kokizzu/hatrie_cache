#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU03_STORED_PROCEDURE_REGISTRY.md \
  Makefile \
  hat/hatProcedure/doc.go \
  hat/hatProcedure/procedure.go \
  hat/hatProcedure/procedure_benchmark_test.go \
  hat/hatProcedure/procedure_test.go \
  scripts/benchmark-round35-procedure.sh \
  scripts/commit-round35-procedure.sh \
  scripts/format-round35-procedure.sh \
  scripts/push-round35-procedure.sh \
  scripts/race-round35-procedure.sh \
  scripts/review-round35-procedure.sh \
  scripts/stage-round35-procedure.sh \
  scripts/test-round35-procedure.sh \
  scripts/verify-round35-procedure-docs.sh \
  scripts/vet-round35-procedure.sh
