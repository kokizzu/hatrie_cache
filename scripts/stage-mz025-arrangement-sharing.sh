#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  hat/hatSql/m_u05_arrangement_recovery.go \
  hat/hatSql/typed_table_arrangement_snapshot.go \
  hat/hatSql/typed_table_arrangement_stats.go \
  hat/hatSql/typed_table_arrangements.go \
  hat/hatSql/mz025_arrangement_sharing_test.go \
  MZ025_ARRANGEMENT_SHARING.md \
  scripts/benchmark-mz025-arrangement-sharing.sh \
  scripts/format-mz025-arrangement-sharing.sh \
  scripts/test-mz025-arrangement-sharing.sh \
  scripts/verify-mz025-arrangement-sharing.sh \
  scripts/stage-mz025-arrangement-sharing.sh \
  scripts/commit-mz025-arrangement-sharing.sh \
  scripts/push-mz025-arrangement-sharing.sh

git add -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  hat/hatSql/m_u05_arrangement_recovery.go \
  hat/hatSql/typed_table_arrangement_snapshot.go \
  hat/hatSql/typed_table_arrangement_stats.go \
  hat/hatSql/typed_table_arrangements.go \
  hat/hatSql/mz025_arrangement_sharing_test.go \
  MZ025_ARRANGEMENT_SHARING.md \
  scripts/benchmark-mz025-arrangement-sharing.sh \
  scripts/format-mz025-arrangement-sharing.sh \
  scripts/test-mz025-arrangement-sharing.sh \
  scripts/verify-mz025-arrangement-sharing.sh \
  scripts/stage-mz025-arrangement-sharing.sh \
  scripts/commit-mz025-arrangement-sharing.sh \
  scripts/push-mz025-arrangement-sharing.sh

git diff --cached --check
