#!/usr/bin/env bash
set -euo pipefail

files=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  Makefile
  T-U14_TYPED_TABLE_FIELD_UPDATES.md
  hat/hatSql/t_u14_typed_table_update_benchmark_test.go
  hat/hatSql/t_u14_typed_table_update_test.go
  hat/hatSql/typed_table_updates.go
  scripts/run-t-u14-typed-table-update.sh
  scripts/verify-t-u14-ship.sh
  scripts/ship-t-u14.sh
)

case "${1:-}" in
  review)
    git diff --check
    git diff --stat -- "${files[@]}"
    git status --short --untracked-files=all
    ;;
  commit)
    git add -- "${files[@]}"
    git diff --cached --check
    git commit -m "feat(sql): add typed table field updates [skip ci]"
    ;;
  push)
    git push origin HEAD
    ;;
  *)
    printf 'usage: %s {review|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
