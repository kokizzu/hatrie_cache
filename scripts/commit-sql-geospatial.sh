#!/usr/bin/env sh
set -eu

make review-sql-geospatial
git add -- Makefile hat/hatSql/geospatial.go hat/hatSql/geospatial_test.go hat/hatSql/temporal_analytics.go hat/hatSql/external_quality_test.go scripts/inspect-temporal-analytics-goal.sh scripts/test-sql-geospatial.sh scripts/test-sql-external-quality.sh scripts/format-sql-geospatial.sh scripts/review-sql-geospatial.sh scripts/commit-sql-geospatial.sh
git diff --cached --check
git commit --only -m 'feat: add SQL geospatial index' -- Makefile hat/hatSql/geospatial.go hat/hatSql/geospatial_test.go hat/hatSql/temporal_analytics.go hat/hatSql/external_quality_test.go scripts/inspect-temporal-analytics-goal.sh scripts/test-sql-geospatial.sh scripts/test-sql-external-quality.sh scripts/format-sql-geospatial.sh scripts/review-sql-geospatial.sh scripts/commit-sql-geospatial.sh
git push
