#!/usr/bin/env sh
set -eu

make review-sql-rollup
git add -- Makefile hat/hatSql/rollup.go hat/hatSql/rollup_test.go scripts/test-sql-rollup.sh scripts/format-sql-rollup.sh scripts/review-sql-rollup.sh scripts/commit-sql-rollup.sh
git diff --cached --check
git commit --only -m 'feat: add SQL time bucket rollups' -- Makefile hat/hatSql/rollup.go hat/hatSql/rollup_test.go scripts/test-sql-rollup.sh scripts/format-sql-rollup.sh scripts/review-sql-rollup.sh scripts/commit-sql-rollup.sh
git push
