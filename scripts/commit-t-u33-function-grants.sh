#!/usr/bin/env bash
set -euo pipefail

git add \
    BENCHMARK.md \
    Makefile \
    PRODUCT_IDEA_GAPS.md \
    README.md \
    TU33_FUNCTION_GRANTS.md \
    hat/hatAuth/rbac.go \
    hat/hatAuth/role_catalog.go \
    hat/hatAuth/tr048_function_grants_test.go \
    scripts/benchmark-t-u33-function-grants.sh \
    scripts/commit-t-u33-function-grants.sh \
    scripts/format-t-u33-function-grants.sh \
    scripts/push-t-u33-function-grants.sh \
    scripts/test-t-u33-function-grants.sh \
    scripts/verify-t-u33-function-grants.sh
git diff --cached --check
git commit -m 'feat: add function grants to hatAuth [skip ci]'
