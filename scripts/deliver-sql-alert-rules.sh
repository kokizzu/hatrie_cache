#!/bin/sh
set -eu

git diff --check
git add -- \
	Makefile \
	hat/hatSql/alert_rules.go \
	hat/hatSql/alert_rules_test.go \
	scripts/test-sql-alert-rules.sh \
	scripts/deliver-sql-alert-rules.sh
git diff --cached --name-only
git commit -m "add SQL query alert rules"
git push
