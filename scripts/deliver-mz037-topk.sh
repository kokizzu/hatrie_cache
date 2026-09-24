#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  Makefile \
  MZ037_TOPK_SCRATCH_REUSE.md \
  PRODUCT_IDEA_GAPS.md \
  hat/hatSql/c213_incremental_top_k.go \
  hat/hatSql/c213_incremental_top_k_test.go \
  scripts/benchmark-c213-topk.sh \
  scripts/deliver-mz037-topk.sh \
  scripts/race-c213-topk.sh \
  scripts/test-c213-topk.sh \
  scripts/test-mz037-package.sh \
  scripts/verify-mz037-topk.sh \
  scripts/vet-mz037-package.sh
git diff --cached --check
git commit -m "perf(sql): reuse incremental top-k selection scratch"
git push origin HEAD:master
