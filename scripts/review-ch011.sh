#!/usr/bin/env bash
set -euo pipefail

files=(
  Makefile
  README.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  CH011_PROJECTION_DDL.md
  hat/hatSql/session.go
  hat/hatSql/materialized.go
  hat/hatSql/ch011_projection_ddl_test.go
  hat/hatSql/ch011_projection_ddl_benchmark_test.go
  scripts/format-ch011-projection-ddl.sh
  scripts/test-ch011-projection-ddl.sh
  scripts/benchmark-ch011-projection-ddl.sh
  scripts/review-ch011.sh
  scripts/stage-ch011-projection-ddl.sh
  scripts/commit-ch011-projection-ddl.sh
  scripts/push-ch011-projection-ddl.sh
)

git diff --check -- "${files[@]}"
test ! -e scripts/inspect-ch011-context.sh
printf '%s\n' 'CH-011 review scope:'
git status --short --untracked-files=all
