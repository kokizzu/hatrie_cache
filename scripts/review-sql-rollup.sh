#!/usr/bin/env sh
set -eu

make format-sql-rollup
make test-sql-rollup
make verify-sql-improvement-goal
git diff --check
git status --short
printf '%s\n' 'SQL rollup review passed'
