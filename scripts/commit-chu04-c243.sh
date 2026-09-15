#!/usr/bin/env bash
set -euo pipefail

is_allowed() {
  case "$1" in
    ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|CHU04_EXTERNAL_DISTINCT_SPILL.md|Makefile|PRODUCT_IDEA_GAPS.md|README.md|hat/hatSql/query.go|hat/hatSql/chu04_external_distinct_spill_test.go|hat/hatSql/chu04_external_distinct_spill_benchmark_test.go|scripts/benchmark-chu04-c243.sh|scripts/commit-chu04-c243.sh|scripts/format-chu04-c243.sh|scripts/memory-chu04-c243.sh|scripts/push-chu04-c243.sh|scripts/race-chu04-c243.sh|scripts/stage-chu04-c243.sh|scripts/test-chu04-c243.sh|scripts/test-chu04-package-c243.sh|scripts/vet-chu04-c243.sh)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

while IFS= read -r path; do
  if [[ -n "$path" ]] && ! is_allowed "$path"; then
    printf 'refusing to commit unrelated staged path: %s\n' "$path" >&2
    exit 1
  fi
done < <(git diff --cached --name-only)
git diff --cached --check
if git diff --cached --quiet; then
  printf '%s\n' 'nothing staged for CH-U04' >&2
  exit 1
fi
printf '%s\n' 'staged CH-U04 paths:'
git diff --cached --name-only
git commit -m 'feat: spill external distinct results'
