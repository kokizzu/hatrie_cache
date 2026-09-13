#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md INSPIRATION_BACKLOG.md BENCHMARK.md TR033_SQL_RETURNING_BEFORE_ROWS.md \
  hat/hatCache/sql.go \
  hat/hatCache/sql_production_test.go \
  hat/hatCache/tr033_returning_old_row_test.go \
  hat/hatCache/tr033_returning_old_row_benchmark_test.go \
  scripts/benchmark-tr033.sh scripts/test-tr033.sh scripts/format-tr033.sh \
  scripts/test-tr033-race.sh scripts/test-tr033-full.sh scripts/review-tr033.sh \
  scripts/commit-tr033.sh scripts/push-tr033.sh
git diff --cached --check
git commit -m "feat: expose SQL mutation before rows"
