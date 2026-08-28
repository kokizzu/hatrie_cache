#!/usr/bin/env sh
set -eu

make format-sql-geospatial
make test-sql-geospatial
make test-temporal-analytics
make test-sql-external-quality
make verify-sql-improvement-goal
git diff --check
git status --short
printf '%s\n' 'SQL geospatial review passed'
