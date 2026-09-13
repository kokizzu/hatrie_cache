#!/usr/bin/env bash
set -eu

git diff --check
git add \
  BENCHMARK.md \
  CH024_SKIP_INDEX_USEFULNESS.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  README.md \
  hat/hatSql/ch024_skip_usefulness_benchmark_test.go \
  hat/hatSql/columnar_segment_skip_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-ch024-after.sh \
  scripts/benchmark-ch024-before.sh \
  scripts/commit-ch024.sh \
  scripts/format-ch024.sh \
  scripts/push-ch024.sh \
  scripts/status-ch024.sh \
  scripts/test-ch024-full.sh \
  scripts/test-ch024-race.sh \
  scripts/test-ch024.sh
git diff --cached --check
git commit -m "feat: expose skip index usefulness telemetry"
