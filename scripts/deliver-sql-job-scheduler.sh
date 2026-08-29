#!/bin/sh
set -eu

git diff --check
git add -- \
	Makefile \
	hat/hatSql/job_scheduler.go \
	hat/hatSql/job_scheduler_test.go \
	scripts/test-sql-job-scheduler.sh \
	scripts/deliver-sql-job-scheduler.sh
git diff --cached --name-only
git commit -m "add scheduled SQL job scheduler"
git push
