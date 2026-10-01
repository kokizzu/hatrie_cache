#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU033_ROLE_FUNCTION_GRANTS.md \
  Makefile \
  hat/hatAuth/role_catalog_stored_function_authorizer.go \
  hat/hatAuth/t_u33_role_function_authorizer_test.go \
  hat/hatAuth/t_u33_role_function_authorizer_benchmark_test.go \
  scripts/test-t-u33.sh \
  scripts/deliver-t-u33.sh
git commit -m 'feat(auth): connect role catalog function grants [skip ci]'
git push origin HEAD
