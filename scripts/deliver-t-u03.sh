#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU003_STORED_FUNCTION_REGISTRY.md \
  Makefile \
  hat/hatAuth/stored_function_registry.go \
  hat/hatAuth/t_u03_stored_function_registry_test.go \
  hat/hatAuth/t_u03_stored_function_registry_benchmark_test.go \
  scripts/test-t-u03.sh \
  scripts/deliver-t-u03.sh
git commit -m 'feat(auth): add versioned stored function registry [skip ci]'
git push origin HEAD
