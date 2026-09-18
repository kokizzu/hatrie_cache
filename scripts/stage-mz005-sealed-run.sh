#!/usr/bin/env bash
set -euo pipefail

git add Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ005_IMMUTABLE_SEALED_UPSERT_RUN.md \
  README.md \
  hat/hatDataStructure/sealed_upsert_run.go \
  hat/hatDataStructure/sealed_upsert_run_test.go \
  hat/hatDataStructure/sealed_upsert_run_benchmark_test.go \
  scripts/benchmark-mz005-sealed-run.sh \
  scripts/commit-mz005-sealed-run.sh \
  scripts/format-mz005-sealed-run.sh \
  scripts/push-mz005-sealed-run.sh \
  scripts/race-mz005-sealed-run.sh \
  scripts/stage-mz005-sealed-run.sh \
  scripts/test-mz005-sealed-run-package.sh \
  scripts/test-mz005-sealed-run.sh \
  scripts/verify-mz005-sealed-run-docs.sh \
  scripts/vet-mz005-sealed-run.sh

git diff --cached --check
git status --short
