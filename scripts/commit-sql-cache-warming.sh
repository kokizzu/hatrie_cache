#!/usr/bin/env sh
set -eu

make review-sql-cache-warming
git add -- Makefile hat/hatSql/cache_warming.go hat/hatSql/cache_warming_test.go scripts/test-sql-cache-warming.sh scripts/format-sql-cache-warming.sh scripts/review-sql-cache-warming.sh scripts/commit-sql-cache-warming.sh
git diff --cached --check
git commit --only -m 'feat: add SQL cache warming registry' -- Makefile hat/hatSql/cache_warming.go hat/hatSql/cache_warming_test.go scripts/test-sql-cache-warming.sh scripts/format-sql-cache-warming.sh scripts/review-sql-cache-warming.sh scripts/commit-sql-cache-warming.sh
git push
