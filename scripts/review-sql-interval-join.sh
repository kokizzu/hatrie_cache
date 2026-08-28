#!/usr/bin/env sh
set -eu

make format-sql-interval-join
make test-sql-interval-join
make test-sql-approximate-aggregates
make test-temporal-analytics
make test-sql-rollup
make verify-sql-improvement-goal
git diff --check
git status --short
printf '%s\n' 'SQL interval join review passed'
