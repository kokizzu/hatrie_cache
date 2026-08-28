#!/usr/bin/env sh
set -eu

make format-sql-analytics-benchmarks
make test-sql-graph
make benchmark-sql-analytics-goal
make verify-sql-improvement-goal
git diff --check
git status --short
printf '%s\n' 'SQL analytics benchmark review passed'
