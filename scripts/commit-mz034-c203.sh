#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  MZ034_GENERIC_NEGATIVE_DIFF.md \
  README.md \
  hat/hatSql/m034_generic_negative_diff_example_test.go \
  scripts/benchmark-mz034-c203.sh \
  scripts/commit-mz034-c203.sh \
  scripts/format-mz034-c203.sh \
  scripts/inspect-mz034-c203.sh \
  scripts/push-mz034-c203.sh \
  scripts/race-mz034-c203.sh \
  scripts/review-mz034-c203.sh \
  scripts/test-mz034-c203.sh \
  scripts/verify-mz034-c203.sh \
  scripts/vet-mz034-c203.sh
printf 'n\ny\n' | git add -p -- Makefile
git diff --cached --check
git commit -m 'docs: record generic differential operators'
