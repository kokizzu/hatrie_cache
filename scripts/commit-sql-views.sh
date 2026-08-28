#!/usr/bin/env sh
set -eu

git add Makefile SQL.md hat/hatSql/session.go hat/hatSql/session_test.go scripts/commit-sql-views.sh
git commit -m 'feat: add session SQL views with cycle checks'
git push
