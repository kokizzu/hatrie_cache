#!/bin/sh
set -eu

mode=${1:-status}
case "$mode" in
stage-fix)
  git add hat/hatSql/m_u05_arrangement_recovery.go
  ;;
commit-fix)
  git commit -m 'fix(sql): materialize arrangement checkpoint keys [skip ci]'
  ;;
stage-feature)
  git add Makefile \
    CHG02_UNIFIED_EXTERNAL_SORT.md \
    hat/hatSql/query.go \
    hat/hatSql/chg02_unified_external_sort_test.go \
    hat/hatSql/chg02_unified_external_sort_benchmark_test.go \
    scripts/format-chg02-star-spill.sh \
    scripts/test-chg02-star-spill.sh \
    scripts/benchmark-chg02-star-spill.sh \
    scripts/race-chg02-star-spill.sh \
    scripts/vet-chg02-star-spill.sh \
    scripts/test-chg02-star-spill-package.sh \
    scripts/deliver-chg02-star-spill.sh
  ;;
commit-feature)
  git commit -m 'feat(sql): spill unbounded SELECT star sorts [skip ci]'
  ;;
push)
  git push --set-upstream origin codex/chg02-unified-sort-v2
  ;;
status)
  git status --short
  ;;
check)
  git diff --check
  git status --short
  ;;
*)
  printf '%s\n' "unknown delivery mode: $mode" >&2
  exit 2
  ;;
esac
