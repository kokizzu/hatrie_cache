#!/usr/bin/env sh
set -eu

git add -- \
    Makefile \
    SQL.md \
    hat/hatCache/sql.go \
    hat/hatCache/sql_test.go \
    hat/hatCache/sql_transaction.go \
    hat/hatCache/sql_function_test.go \
    scripts/audit-next-sql-improvements.sh \
    scripts/inspect-sql-engine.sh \
    scripts/show-sql-mutation-engine.sh \
    scripts/show-sql-mutation-tests.sh \
    scripts/show-hattrie-core.sh \
    scripts/show-scalar-command-path.sh \
    scripts/test-sql-mutations.sh \
    scripts/format-sql-mutations.sh \
    scripts/show-sql-mutation-docs.sh \
    scripts/show-sql-keyword-inventory.sh \
    scripts/verify-sql-mutation-feature.sh \
    scripts/commit-sql-mutation-feature.sh
git commit -m 'feat: add SQL merge returning and savepoints'
git push
