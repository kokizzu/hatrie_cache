#!/usr/bin/env sh
set -eu

make format-sql-columnar-numeric-filter
make test-sql-columnar-scan
make benchmark-sql-columnar-scan
make verify-sql-improvement-goal
git diff --check
git status --short
printf '%s\n' 'SQL columnar numeric filter review passed'
