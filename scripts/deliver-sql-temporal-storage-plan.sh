#!/bin/sh
set -eu

git diff --check
git diff -- ADOPTED_QUERY_ENGINE_IDEAS.md Makefile hat/hatSql/temporal_analytics.go hat/hatSql/temporal_analytics_benchmark_test.go hat/hatSql/temporal_analytics_order_test.go scripts/test-sql-temporal-storage.sh scripts/benchmark-sql-temporal-storage.sh scripts/verify-sql-temporal-storage.sh scripts/deliver-sql-temporal-storage-plan.sh scripts/deliver-sql-temporal-storage.sh
