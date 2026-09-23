#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  M214_SORTED_ARRANGEMENT_REUSE.md
  hat/hatSql/typed_table_sorted_arrangements.go
  hat/hatSql/m214_sorted_arrangements_test.go
  hat/hatSql/m214_sorted_arrangements_benchmark_test.go
  scripts/m214-test.sh
  scripts/m214-benchmark.sh
  scripts/m214-format.sh
  scripts/m214-race.sh
  scripts/m214-vet.sh
  scripts/m214-stage.sh
  scripts/m214-commit.sh
  scripts/m214-push.sh
)

if [[ -n "$(git diff --cached --name-only)" ]]; then
  printf '%s\n' 'Refusing to stage M214 with pre-existing staged changes:'
  git diff --cached --name-only
  exit 1
fi

git status --short -- "${paths[@]}"
git diff --check -- "${paths[@]}"
git add -- "${paths[@]}"
git diff --cached --check
git status --short -- "${paths[@]}"
