#!/usr/bin/env sh
set -eu

git add -- \
    Makefile \
    SQL.md \
    hat/hatSql/plan_guard.go \
    hat/hatSql/plan_guard_test.go \
    scripts/show-sql-relational-extension-points.sh \
    scripts/check-next-sql-feature-symbols.sh \
    scripts/show-sql-source-parser.sh \
    scripts/show-sql-source-execution.sh \
    scripts/test-sql-plan-guards.sh \
    scripts/format-sql-plan-guards.sh \
    scripts/commit-sql-plan-guard.sh
git commit -m 'feat: add SQL plan regression guards'
git push
