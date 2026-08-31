#!/bin/sh
set -eu

paths='Makefile PROJECTION_ADVISOR.md README.md hat/hatSql/projection_advisor.go hat/hatSql/projection_advisor_benchmark_test.go hat/hatSql/projection_advisor_test.go hat/hatSql/query.go scripts/audit-sql-projection-advisor-security.sh scripts/benchmark-sql-projection-advisor.sh scripts/deliver-sql-projection-advisor.sh scripts/format-sql-projection-advisor.sh scripts/inspect-sql-advisor-contracts.sh scripts/test-race-sql-projection-advisor.sh scripts/test-sql-projection-advisor.sh scripts/verify-sql-projection-advisor-docs.sh'

if ! git diff --cached --quiet; then
	echo 'refusing to deliver with pre-existing staged changes' >&2
	exit 1
fi

git diff --check -- $paths
git add -- $paths
git diff --cached --check
git commit -m 'feat: advise SQL materialized projections'
git push
