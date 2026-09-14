#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf '%s\n' 'Refusing to commit because the index already contains unrelated staged changes.' >&2
  exit 1
fi

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  DIFFERENTIAL_GROUP_BY.md \
  DIFFERENTIAL_OPERATORS.md \
  INSPIRATION.md \
  hat/hatSql/differential_average.go \
  hat/hatSql/differential_average_test.go \
  hat/hatSql/differential_average_benchmark_test.go \
  scripts/benchmark-differential-average.sh \
  scripts/commit-differential-average.sh \
  scripts/format-differential-average.sh \
  scripts/push-differential-average.sh \
  scripts/review-differential-average.sh \
  scripts/test-differential-average.sh \
  scripts/verify-differential-average.sh

makefile_patch=$(mktemp)
trap 'rm -f "$makefile_patch"' EXIT
printf '%s\n' \
  'diff --git a/Makefile b/Makefile' \
  '--- a/Makefile' \
  '+++ b/Makefile' \
  '@@ -13808,3 +13808,30 @@' \
  ' .PHONY: push-mz044-snapshot-token' \
  ' push-mz044-snapshot-token:' \
  $' \t@bash ./scripts/push-mz044-snapshot-token.sh' \
  '+' \
  '+.PHONY: test-differential-average' \
  '+test-differential-average:' \
  $'+\t@bash scripts/test-differential-average.sh' \
  '+.PHONY: benchmark-differential-average' \
  '+benchmark-differential-average:' \
  $'+\t@bash scripts/benchmark-differential-average.sh' \
  '+' \
  '+.PHONY: format-differential-average' \
  '+format-differential-average:' \
  $'+\t@bash scripts/format-differential-average.sh' \
  '+' \
  '+.PHONY: verify-differential-average' \
  '+verify-differential-average:' \
  $'+\t@bash scripts/verify-differential-average.sh' \
  '+' \
  '+.PHONY: review-differential-average' \
  '+review-differential-average:' \
  $'+\t@bash scripts/review-differential-average.sh' \
  '+' \
  '+.PHONY: commit-differential-average' \
  '+commit-differential-average:' \
  $'+\t@bash scripts/commit-differential-average.sh' \
  '+' \
  '+.PHONY: push-differential-average' \
  '+push-differential-average:' \
  $'+\t@bash scripts/push-differential-average.sh' \
  > "$makefile_patch"
git apply --cached "$makefile_patch"

git diff --cached --check
git diff --cached --name-only
git commit -m 'feat(sql): add differential average maintenance'
