#!/usr/bin/env bash
set -eu

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  SQL_IP_TYPES.md \
  Makefile \
  hat/hatCache/ip_sql_test.go \
  hat/hatCache/sql.go \
  hat/hatCache/sql_query.go \
  hat/hatSchema/generate.go \
  hat/hatSchema/ip_types_test.go \
  hat/hatSchema/schema.go \
  hat/hatSql/ip_row_binary_baseline_test.go \
  hat/hatSql/ip_types.go \
  hat/hatSql/ip_types_test.go \
  hat/hatSql/query.go \
  hat/hatSql/row_binary.go \
  hat/hatSql/row_binary_nullable_bitmap.go \
  hat/hatSql/row_binary_read_stats.go \
  hat/hatSql/row_binary_stats.go \
  hat/hatSql/row_binary_stream.go \
  scripts/benchmark-sql-ip-types-after.sh \
  scripts/benchmark-sql-ip-types-before.sh \
  scripts/check-sql-ip-types.sh \
  scripts/commit-sql-ip-types.sh \
  scripts/format-sql-ip-types.sh \
  scripts/push-sql-ip-types.sh \
  scripts/race-sql-ip-types.sh \
  scripts/review-sql-ip-types.sh \
  scripts/test-sql-ip-types-integration.sh \
  scripts/test-sql-ip-types.sh \
  scripts/vet-sql-ip-types.sh
git commit -m "feat: add typed IPv4 and IPv6 codecs"
