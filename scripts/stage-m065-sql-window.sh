#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile README.md INSPIRATION_ROUND2.md BENCHMARK.md M065_SQL_INCREMENTAL_WINDOW.md \
	hat/hatSql/query.go hat/hatSql/m065_sql_incremental_window.go hat/hatSql/m065_sql_incremental_window_test.go \
	scripts/test-m065-sql-window.sh scripts/test-m065-sql-window-package.sh \
	scripts/benchmark-m065-sql-window.sh scripts/format-m065-sql-window.sh \
	scripts/race-m065-sql-window.sh scripts/vet-m065-sql-window.sh \
	scripts/stage-m065-sql-window.sh scripts/commit-m065-sql-window.sh \
	scripts/push-m065-sql-window.sh
git diff --cached --check
git diff --cached --stat
git status --short
