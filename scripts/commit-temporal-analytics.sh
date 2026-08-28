#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/temporal_analytics.go hat/hatSql/temporal_analytics_test.go scripts/test-temporal-analytics.sh scripts/format-temporal-analytics.sh scripts/review-temporal-analytics.sh scripts/commit-temporal-analytics.sh
git add Makefile hat/hatSql/temporal_analytics.go hat/hatSql/temporal_analytics_test.go scripts/test-temporal-analytics.sh scripts/format-temporal-analytics.sh scripts/review-temporal-analytics.sh scripts/commit-temporal-analytics.sh
git diff --cached --check
git commit -m 'feat: add temporal analytics primitives'
git push
