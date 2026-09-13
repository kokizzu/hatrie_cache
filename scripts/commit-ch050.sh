#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH050_SQL_ROW_BINARY_STREAM.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  README.md \
  hat/hatCache/monitoring.go \
  hat/hatCache/sql_row_binary_http.go \
  hat/hatCache/sql_row_binary_http_test.go \
  hat/hatSql/client.go \
  hat/hatSql/row_binary_stream.go \
  hat/hatSql/row_binary_stream_benchmark_data_test.go \
  hat/hatSql/row_binary_stream_benchmark_test.go \
  hat/hatSql/row_binary_stream_ndjson_benchmark_test.go \
  hat/hatSql/row_binary_stream_test.go \
  scripts/benchmark-ch050-after.sh \
  scripts/benchmark-ch050-before.sh \
  scripts/check-ch050.sh \
  scripts/commit-ch050.sh \
  scripts/format-ch050.sh \
  scripts/push-ch050.sh \
  scripts/race-ch050.sh \
  scripts/test-ch050-all.sh \
  scripts/test-ch050-row-binary-stream-red.sh \
  scripts/test-ch050.sh \
  scripts/vet-ch050.sh
git diff --cached --check
git commit -m 'chore: add CH050 commit and push wrappers'
