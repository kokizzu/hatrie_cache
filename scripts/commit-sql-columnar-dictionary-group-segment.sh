#!/bin/sh
set -eu

git diff --check -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_columnar_dictionary_group_segment_benchmark_test.go \
  hat/hatCache/sql_columnar_dictionary_group_segment_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-sql-columnar-dictionary-group-segment.sh \
  scripts/commit-sql-columnar-dictionary-group-segment.sh \
  scripts/format-sql-columnar-dictionary-group-segment.sh \
  scripts/test-sql-columnar-dictionary-group-segment.sh
git add -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_columnar_dictionary_group_segment_benchmark_test.go \
  hat/hatCache/sql_columnar_dictionary_group_segment_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-sql-columnar-dictionary-group-segment.sh \
  scripts/commit-sql-columnar-dictionary-group-segment.sh \
  scripts/format-sql-columnar-dictionary-group-segment.sh \
  scripts/test-sql-columnar-dictionary-group-segment.sh
git commit --only -m 'perf: skip SQL group aggregate segments' -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_columnar_dictionary_group_segment_benchmark_test.go \
  hat/hatCache/sql_columnar_dictionary_group_segment_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-sql-columnar-dictionary-group-segment.sh \
  scripts/commit-sql-columnar-dictionary-group-segment.sh \
  scripts/format-sql-columnar-dictionary-group-segment.sh \
  scripts/test-sql-columnar-dictionary-group-segment.sh
git push
