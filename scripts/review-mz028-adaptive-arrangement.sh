#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' '## changed paths'
git status --short
printf '%s\n' '## MZ-028 documentation references'
printf '%s\n' '## MZ-028 report'
sed -n '1,90p' MZ028_ADAPTIVE_ARRANGEMENT.md
printf '%s\n' '## MZ-028 benchmark section'
sed -n '27000,27026p' BENCHMARK.md
printf '%s\n' '## diff stat'
git diff --stat -- \
  Makefile \
  hat/hatSql/typed_table_aggregate_dictionary.go \
  hat/hatSql/mz028_adaptive_arrangement_test.go \
  hat/hatSql/mz028_batched_merge_test.go \
  scripts/benchmark-mz028-batched-merge.sh \
  scripts/format-mz028-adaptive-arrangement.sh \
  scripts/race-mz028-adaptive-arrangement.sh \
  scripts/review-mz028-adaptive-arrangement.sh \
  scripts/test-mz028-adaptive-arrangement.sh \
  scripts/test-mz028-batched-merge.sh \
  scripts/test-mz028-package.sh \
  scripts/vet-mz028-adaptive-arrangement.sh
printf '%s\n' '## staged paths'
git diff --cached --check
git diff --cached --name-status
git diff --cached --stat
printf '%s\n' '## staged delivery-script changes'
git diff --cached -- scripts/commit-mz028-adaptive-arrangement.sh scripts/stage-mz028-adaptive-arrangement.sh
