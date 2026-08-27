#!/bin/sh
set -eu

git add BENCHMARK.md Makefile SQL.md hat/hatSql/approx_aggregate.go hat/hatSql/approx_aggregate_test.go hat/hatSql/query.go scripts/benchmark-sql-approx-aggregates.sh scripts/commit-sql-approx-feature.sh scripts/format-sql-approx-aggregates.sh scripts/inspect-sql-approx-feature.sh scripts/push-sql-approx-feature.sh scripts/show-benchmark-sql-section.sh scripts/show-new-packages.sh scripts/show-sql-aggregate-docs.sh scripts/show-sql-approx-engine.sh scripts/show-sql-function-dispatch.sh scripts/test-sql-approx-aggregates.sh scripts/verify-sql-approx-feature.sh
git commit -m 'feat: add SQL approximate aggregates'
