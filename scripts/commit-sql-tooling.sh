#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
git diff --check
git add Makefile hat/hatSql/tooling.go hat/hatSql/tooling_test.go scripts/test-sql-tooling.sh scripts/format-sql-tooling.sh scripts/commit-sql-tooling.sh scripts/push-sql-tooling.sh
git diff --cached --check
git commit -m "feat: add SQL editor tooling"
