#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -count=1
go test -race ./hat/hatSql -run '^TestCH007' -count=1
go vet ./hat/hatSql
git diff --check -- \
  Makefile \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH007_ROW_TTL.md \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_patch_parts.go \
  hat/hatSql/typed_table_stats.go \
  hat/hatSql/typed_table_histogram.go \
  hat/hatSql/typed_table_ttl.go \
  hat/hatSql/ch007_row_ttl_test.go \
  hat/hatSql/ch007_row_ttl_benchmark_test.go \
  scripts/format-ch007-row-ttl-c203.sh \
  scripts/benchmark-ch007-row-ttl-c203.sh \
  scripts/verify-ch007-row-ttl-c203.sh
