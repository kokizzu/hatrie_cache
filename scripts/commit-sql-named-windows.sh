#!/bin/sh
set -eu

git add -- Makefile ./hat/hatSql/query.go ./hat/hatSql/named_window_test.go ./scripts/test-sql-named-windows.sh ./scripts/format-sql-named-windows.sh ./scripts/review-sql-named-windows.sh ./scripts/commit-sql-named-windows.sh
git commit -m "feat: add named SQL windows"
git push
