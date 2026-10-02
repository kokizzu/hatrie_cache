#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add Makefile PRODUCT_IDEA_GAPS.md MU035_SNAPSHOT_BLOCKING.md \
  hat/hatSql/m_u35_snapshot_blocking.go \
  hat/hatSql/m_u35_snapshot_blocking_test.go \
  hat/hatSql/m_u35_snapshot_blocking_benchmark_test.go \
  hat/hatSql/sql_source_frontier.go \
  scripts/test-mu35-red.sh scripts/format-mu35.sh scripts/benchmark-mu35.sh \
  scripts/race-mu35.sh scripts/vet-mu35.sh scripts/test-mu35-package.sh \
  scripts/test-mu35-all.sh scripts/status-mu35.sh scripts/deliver-mu35.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat(sql): add snapshot blocking semantics [skip ci]'
git push -u origin codex/mu35-snapshot-blocking
