#!/usr/bin/env bash
set -euo pipefail

git add \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INSPIRATION.md \
  Makefile \
  README.md \
  SQL_SNAPSHOT_PROVIDER.md \
  hat/hatSql/query.go \
  hat/hatSql/sql_snapshot_provider.go \
  hat/hatSql/sql_snapshot_provider_benchmark_test.go \
  hat/hatSql/sql_snapshot_provider_test.go \
  scripts/benchmark-sql-snapshot-provider.sh \
  scripts/commit-sql-snapshot-provider.sh \
  scripts/format-sql-snapshot-provider.sh \
  scripts/push-sql-snapshot-provider.sh \
  scripts/review-sql-snapshot-provider.sh \
  scripts/test-sql-snapshot-provider.sh
git diff --cached --check
git commit -m "feat(sql): add opt-in consistent snapshot provider"
