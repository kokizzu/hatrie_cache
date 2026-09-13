#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  CH050_SQL_ROW_BINARY_STREAM.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  README.md \
  hat/hatCache/ch050_row_binary_import_benchmark_test.go \
  hat/hatCache/ch050_row_binary_import_test.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/sql.go \
  hat/hatCache/sql_row_binary_http.go \
  hat/hatCache/sql_row_binary_import.go \
  hat/hatSql/row_binary_stream.go \
  scripts/benchmark-ch050-baseline.sh \
  scripts/benchmark-ch050-rowbinary-import.sh \
  scripts/commit-ch050-rowbinary-import.sh \
  scripts/format-ch050-rowbinary-import.sh \
  scripts/measure-ch050-rowbinary-import.sh \
  scripts/push-ch050-rowbinary-import.sh \
  scripts/review-ch050-rowbinary-import.sh
git diff --cached --check
git commit -m 'feat: add SQL RowBinary bulk import'
