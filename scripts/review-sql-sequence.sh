#!/usr/bin/env sh
set -eu

make format-sql-sequence
make test-sql-sequence
make verify-sql-improvement-goal
git diff --check
git status --short
printf '%s\n' 'SQL sequence review passed'
