#!/usr/bin/env bash
set -euo pipefail

files=(
  Makefile
  README.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CH010_MATERIALIZED_DEFAULT_COLUMNS.md
  hat/hatSql/typed_table.go
  hat/hatSql/ch010_materialized_default_test.go
  hat/hatSql/ch010_materialized_default_benchmark_test.go
  scripts/format-ch010-materialized-default.sh
  scripts/test-ch010-materialized-default.sh
  scripts/benchmark-ch010-materialized-default.sh
  scripts/review-ch010.sh
  scripts/stage-ch010-materialized-default.sh
  scripts/commit-ch010-materialized-default.sh
  scripts/push-ch010-materialized-default.sh
)

git diff --check -- "${files[@]}"
test ! -e scripts/inspect-ch010-context.sh
printf '%s\n' 'CH-010 review scope:'
git status --short --untracked-files=all
