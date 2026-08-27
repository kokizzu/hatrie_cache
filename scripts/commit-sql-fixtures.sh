#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
git diff --check
git add Makefile hat/hatSql/fixture.go hat/hatSql/fixture_test.go hat/hatSql/fixtures/basic-cache.json scripts/test-sql-fixtures.sh scripts/format-sql-fixtures.sh scripts/commit-sql-fixtures.sh scripts/push-sql-fixtures.sh
git diff --cached --check
git commit -m "feat: embed reproducible SQL fixtures"
