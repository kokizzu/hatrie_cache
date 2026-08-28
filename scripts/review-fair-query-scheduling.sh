#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/governance.go hat/hatSql/governance_test.go scripts/test-hat-sql-governance.sh scripts/format-hat-sql-governance.sh scripts/review-fair-query-scheduling.sh scripts/commit-fair-query-scheduling.sh
git diff --name-status -- Makefile hat/hatSql/governance.go hat/hatSql/governance_test.go scripts/test-hat-sql-governance.sh scripts/format-hat-sql-governance.sh scripts/review-fair-query-scheduling.sh scripts/commit-fair-query-scheduling.sh
