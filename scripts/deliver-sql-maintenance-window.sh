#!/bin/sh
set -eu

git diff --check
git add -- \
	Makefile \
	hat/hatSql/maintenance_window.go \
	hat/hatSql/maintenance_window_test.go \
	scripts/test-sql-maintenance-window.sh \
	scripts/deliver-sql-maintenance-window.sh
git diff --cached --name-only
git commit -m "add SQL maintenance windows"
git push
