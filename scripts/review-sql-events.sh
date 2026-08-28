#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/events.go hat/hatSql/events_test.go scripts/test-sql-events.sh scripts/format-sql-events.sh scripts/review-sql-events.sh scripts/commit-sql-events.sh
git diff --name-status -- Makefile hat/hatSql/events.go hat/hatSql/events_test.go scripts/test-sql-events.sh scripts/format-sql-events.sh scripts/review-sql-events.sh scripts/commit-sql-events.sh
