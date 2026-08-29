#!/usr/bin/env sh
set -eu

make format-sql-cache-warming
make test-sql-cache-warming
make verify-sql-improvement-goal
git diff --check
git status --short
printf '%s\n' 'SQL cache warming review passed'
