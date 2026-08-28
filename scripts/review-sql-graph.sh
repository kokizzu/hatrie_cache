#!/usr/bin/env sh
set -eu

make format-sql-graph
make test-sql-graph
make verify-sql-improvement-goal
git diff --check
git status --short
printf '%s\n' 'SQL graph review passed'
