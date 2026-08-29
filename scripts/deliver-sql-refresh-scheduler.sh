#!/bin/sh
set -eu

git diff --check
git add -- \
	Makefile \
	hat/hatSql/refresh_scheduler.go \
	hat/hatSql/refresh_scheduler_test.go \
	scripts/test-sql-refresh-scheduler.sh \
	scripts/deliver-sql-refresh-scheduler.sh
git diff --cached --name-only
git commit -m "add managed SQL refresh schedules"
git push
