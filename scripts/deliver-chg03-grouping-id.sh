#!/usr/bin/env bash
set -euo pipefail

action=${1:-check}

case "$action" in
  check)
    git diff --check
    git diff --cached --check
    git diff --stat
    git diff --cached --stat
    git status --short
    ;;
  stage)
    git add -- \
      BENCHMARK.md \
      ENGINE_IDEAS.md \
      SQL_GROUPING_IDENTIFIERS.md \
      Makefile \
      hat/hatSql/grouping_identifier_benchmark_test.go \
      hat/hatSql/grouping_identifier_test.go \
      hat/hatSql/grouping_sets.go \
      hat/hatSql/query.go \
      scripts/benchmark-chg03-grouping-id.sh \
      scripts/deliver-chg03-grouping-id.sh \
      scripts/format-chg03-grouping-id.sh \
      scripts/race-chg03-grouping-id.sh \
      scripts/test-chg03-grouping-id.sh \
      scripts/vet-chg03-grouping-id.sh
    ;;
  commit-feature)
    git commit -m "feat(sql): add multi-argument grouping id [skip ci]"
    ;;
  push)
    git push -u origin HEAD
    ;;
  *)
    printf 'usage: %s {check|stage|commit-feature|push}\n' "$0" >&2
    exit 2
    ;;
esac
