#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  M215_DELTA_JOIN_MAINTENANCE.md
  hat/hatSql/typed_table_join.go
  hat/hatSql/typed_table_join_arrangements.go
  hat/hatSql/typed_table_join_deltas.go
  hat/hatSql/m215_join_deltas_test.go
  hat/hatSql/m215_join_deltas_benchmark_test.go
  scripts/m215-test.sh
  scripts/m215-benchmark.sh
  scripts/m215-format.sh
  scripts/m215-race.sh
  scripts/m215-vet.sh
  scripts/m215-stage.sh
  scripts/m215-commit.sh
  scripts/m215-push.sh
)

if [[ -n "$(git diff --cached --name-only)" ]]; then
  printf '%s\n' 'Refusing to stage M215 with pre-existing staged changes:'
  git diff --cached --name-only
  exit 1
fi

git status --short -- "${paths[@]}"
git diff --check -- "${paths[@]}"
git add -- "${paths[@]}"
git diff --cached --check
git status --short -- "${paths[@]}"
