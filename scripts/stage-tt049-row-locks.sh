#!/usr/bin/env bash
set -euo pipefail

git add Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  README.md \
  TT049_ROW_LOCK_LEASES.md \
  hat/hatSql/tt049_row_locks.go \
  hat/hatSql/tt049_row_locks_benchmark_test.go \
  hat/hatSql/tt049_row_locks_public_test.go \
  hat/hatSql/tt049_row_locks_test.go \
  scripts/benchmark-tt049-row-locks.sh \
  scripts/format-tt049-row-locks.sh \
  scripts/race-tt049-row-locks.sh \
  scripts/review-tt049.sh \
  scripts/stage-tt049-row-locks.sh \
  scripts/test-tt049-package.sh \
  scripts/test-tt049-row-locks.sh \
  scripts/vet-tt049-row-locks.sh \
  scripts/commit-tt049-row-locks.sh \
  scripts/push-tt049-row-locks.sh
