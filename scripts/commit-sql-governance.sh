#!/bin/sh
set -eu

git add Makefile hat/hatSql/governance.go hat/hatSql/governance_test.go scripts/format-sql-governance.sh scripts/test-sql-governance.sh
git commit -m 'feat: govern SQL resources by namespace'
