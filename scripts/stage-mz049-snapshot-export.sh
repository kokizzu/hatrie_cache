#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  MZ049_SNAPSHOT_EXPORT.md \
  README.md \
  hat/hatCache/snapshot_export_resume.go \
  hat/hatCache/snapshot_export_resume_benchmark_test.go \
  hat/hatCache/snapshot_export_resume_test.go \
  hat/hatSql/explain_arrangement.go \
  hat/hatSql/m_u05_arrangement_recovery.go \
  scripts/benchmark-mz049-snapshot-export.sh \
  scripts/commit-mz049-snapshot-export.sh \
  scripts/format-mz049-snapshot-export.sh \
  scripts/push-mz049-snapshot-export.sh \
  scripts/race-mz049-snapshot-export.sh \
  scripts/stage-mz049-snapshot-export.sh \
  scripts/test-mz049-snapshot-export.sh \
  scripts/verify-mz049-snapshot-export.sh

git diff --cached --check
git status --short
