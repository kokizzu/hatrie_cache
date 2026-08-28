#!/usr/bin/env sh
set -eu

git add Makefile hat/hatSql/session.go hat/hatSql/session_test.go scripts/commit-sql-session.sh scripts/format-sql-session.sh scripts/test-sql-session.sh
git commit -m 'feat: add session-local SQL sources'
git push
