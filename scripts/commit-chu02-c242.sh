#!/usr/bin/env bash
set -euo pipefail

is_allowed() {
  case "$1" in
    ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|CHU02_EXTERNAL_ORDER_SPILL.md|Makefile|PRODUCT_IDEA_GAPS.md|README.md|hat/hatSql/contracts.go|hat/hatSql/external.go|hat/hatSql/query.go|hat/hatSql/chu02_external_order_spill_test.go|hat/hatSql/chu02_external_order_spill_benchmark_test.go|scripts/benchmark-chu02-c242.sh|scripts/commit-chu02-c242.sh|scripts/format-chu02-c242.sh|scripts/memory-chu02-c242.sh|scripts/push-chu02-c242.sh|scripts/race-chu02-c242.sh|scripts/stage-chu02-c242.sh|scripts/test-chu02-c242.sh|scripts/test-chu02-package-c242.sh|scripts/vet-chu02-c242.sh)
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
  printf '%s\n' 'nothing staged for CH-U02' >&2
  exit 1
fi
printf '%s\n' 'staged CH-U02 paths:'
git diff --cached --name-only
git commit -m 'feat: stream external order-by spill'
