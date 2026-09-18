#!/usr/bin/env bash
set -eu

git add Makefile \
  BENCHMARK.md \
  CH047_REMOTE_TABLE_FUNCTIONS.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  hat/hatSql/ch047_remote_table_function.go \
  hat/hatSql/ch047_remote_table_function_benchmark_test.go \
  hat/hatSql/ch047_remote_table_function_test.go \
  scripts/benchmark-ch047-remote-table-function.sh \
  scripts/commit-ch047-remote-table-function.sh \
  scripts/format-ch047-remote-table-function.sh \
  scripts/push-ch047-remote-table-function.sh \
  scripts/race-ch047-remote-table-function.sh \
  scripts/review-ch047-remote-table-function.sh \
  scripts/stage-ch047-remote-table-function.sh \
  scripts/test-ch047-remote-table-function.sh \
  scripts/verify-ch047-remote-table-function-docs.sh \
  scripts/vet-ch047-remote-table-function.sh
