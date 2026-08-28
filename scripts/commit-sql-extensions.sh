#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/extensions.go hat/hatSql/extensions_test.go scripts/audit-extensibility-goal.sh scripts/test-sql-extensions.sh scripts/format-sql-extensions.sh scripts/review-sql-extensions.sh scripts/commit-sql-extensions.sh
git add Makefile hat/hatSql/extensions.go hat/hatSql/extensions_test.go scripts/audit-extensibility-goal.sh scripts/test-sql-extensions.sh scripts/format-sql-extensions.sh scripts/review-sql-extensions.sh scripts/commit-sql-extensions.sh
git diff --cached --check
git commit -m 'feat: add SQL extension registry and capabilities'
git push
