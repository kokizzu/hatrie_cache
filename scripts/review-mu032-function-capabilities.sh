#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  Makefile \
  README.md \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  MU032_UDF_CAPABILITIES.md \
  hat/hatSql/function.go \
  hat/hatSql/registry.go \
  hat/hatCache/sql_function.go \
  hat/hatSql/mu032_function_capabilities_test.go \
  hat/hatSql/mu032_function_capabilities_benchmark_test.go \
  scripts/benchmark-mu032-function-capabilities-baseline.sh \
  scripts/benchmark-mu032-function-capabilities.sh \
  scripts/commit-mu032-function-capabilities.sh \
  scripts/format-mu032-function-capabilities.sh \
  scripts/push-mu032-function-capabilities.sh \
  scripts/race-mu032-function-capabilities.sh \
  scripts/review-mu032-function-capabilities.sh \
  scripts/stage-mu032-function-capabilities.sh \
  scripts/test-mu032-function-capabilities.sh \
  scripts/test-mu032-package.sh \
  scripts/verify-mu032-function-capabilities.sh \
  scripts/vet-mu032-function-capabilities.sh
git status --short -- \
  Makefile \
  README.md \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  MU032_UDF_CAPABILITIES.md \
  hat/hatSql/function.go \
  hat/hatSql/registry.go \
  hat/hatCache/sql_function.go \
  hat/hatSql/mu032_function_capabilities_test.go \
  hat/hatSql/mu032_function_capabilities_benchmark_test.go \
  scripts/benchmark-mu032-function-capabilities-baseline.sh \
  scripts/benchmark-mu032-function-capabilities.sh \
  scripts/commit-mu032-function-capabilities.sh \
  scripts/format-mu032-function-capabilities.sh \
  scripts/push-mu032-function-capabilities.sh \
  scripts/race-mu032-function-capabilities.sh \
  scripts/review-mu032-function-capabilities.sh \
  scripts/test-mu032-function-capabilities.sh \
  scripts/test-mu032-package.sh \
  scripts/verify-mu032-function-capabilities.sh \
  scripts/vet-mu032-function-capabilities.sh
