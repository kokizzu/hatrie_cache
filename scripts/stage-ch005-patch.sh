#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH005_DELETE_BITMAP_SNAPSHOT.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatSql/typed_table_patch_snapshot.go \
  hat/hatSql/ch005_patch_snapshot_test.go \
  hat/hatSql/ch005_patch_snapshot_benchmark_test.go \
  scripts/benchmark-ch005-patch-snapshot.sh \
  scripts/format-ch005-patch.sh \
  scripts/test-ch005-patch-snapshot.sh \
  scripts/review-ch005-snapshot.sh \
  scripts/stage-ch005-patch.sh \
  scripts/commit-ch005-patch.sh \
  scripts/push-ch005-patch.sh

git diff --cached --check
