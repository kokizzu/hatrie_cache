#!/usr/bin/env bash
set -euo pipefail

mode=${1:-stage}
branch="codex/tu21-space-migration"
files=(
  BENCHMARK.md
  PRODUCT_IDEA_GAPS.md
  README.md
  TU21_SPACE_MIGRATION.md
  Makefile
  hat/hatSchema/tu21_space_migration.go
  hat/hatSchema/tu21_space_migration_test.go
  hat/hatSchema/tu21_space_migration_baseline_benchmark_test.go
  hat/hatSchema/tu21_space_migration_benchmark_test.go
  scripts/benchmark-tu21-before.sh
  scripts/benchmark-tu21.sh
  scripts/format-tu21.sh
  scripts/race-tu21.sh
  scripts/test-tu21-before.sh
  scripts/test-tu21-package.sh
  scripts/test-tu21.sh
  scripts/verify-tu21.sh
  scripts/deliver-tu21.sh
)

stage() {
  git add -- "${files[@]}"
  git diff --cached --check
  git diff --cached --name-status
}

case "$mode" in
stage)
  stage
  ;;
commit)
  stage
  git commit -m "feat(schema): add resumable space migration manager [skip ci]"
  ;;
push)
  git push -u origin "$branch"
  ;;
*)
  printf 'usage: %s stage|commit|push\n' "$0" >&2
  exit 2
  ;;
esac
