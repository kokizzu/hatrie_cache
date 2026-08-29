#!/bin/sh
set -eu

git diff --check
git add -- \
	Makefile \
	hat/hatSql/index_advisor.go \
	hat/hatSql/index_advisor_test.go \
	hat/hatSql/index_usage.go \
	hat/hatSql/index_usage_test.go \
	hat/hatSql/query.go \
	scripts/test-sql-index-advisor.sh \
	scripts/test-sql-index-usage.sh \
	scripts/deliver-sql-index-intelligence.sh
git diff --cached --name-only
git commit -m "add SQL index intelligence reports"
git push
