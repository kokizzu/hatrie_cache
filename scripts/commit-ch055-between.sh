#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md CH055_PREPARED_BETWEEN.md INSPIRATION_BACKLOG.md Makefile README.md \
  hat/hatSql/between_program.go hat/hatSql/ch055_between_program_test.go hat/hatSql/query.go \
  scripts/benchmark-ch055-between.sh scripts/commit-ch055-between.sh \
  scripts/format-ch055-between.sh scripts/push-ch055-between.sh \
  scripts/race-ch055-between.sh scripts/test-ch055-between.sh scripts/vet-ch055-between.sh
git commit -m "sql: prepare literal BETWEEN bounds"
