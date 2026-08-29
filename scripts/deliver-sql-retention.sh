#!/bin/sh
set -eu

git diff --check
git add -- \
	Makefile \
	hat/hatSql/retention.go \
	hat/hatSql/retention_test.go \
	scripts/test-sql-retention.sh \
	scripts/deliver-sql-retention.sh
git diff --cached --name-only
git commit -m "add retention archival job"
git push
