#!/bin/sh
set -eu

git add -- Makefile ./hat/hatSql/query.go ./hat/hatSql/time_zone.go ./hat/hatSql/time_zone_test.go ./scripts/test-sql-time-zones.sh ./scripts/format-sql-time-zones.sh ./scripts/review-sql-time-zones.sh ./scripts/commit-sql-time-zones.sh
git commit -m "feat: add SQL time zone arithmetic"
git push
