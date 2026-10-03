#!/usr/bin/env bash
set -euo pipefail

case "${1:-status}" in
  status)
    git status --short
    git diff --check
    git diff --stat
    ;;
  commit-push)
    git diff --check
    git add \
      BENCHMARK.md \
      INSPIRATION.md \
      M090D_NATIVE_SOURCE_SNAPSHOTS.md \
      Makefile \
      hat/hatSql/m052p_auto_native_dataflow.go \
      hat/hatSql/m090d_source_snapshot_benchmark_test.go \
      scripts/m090d-commit-push.sh \
      scripts/m090d-native-source-snapshots.sh
    git diff --cached --check
    git commit -m 'feat(sql): share native source snapshots per query [skip ci]'
    git push -u origin HEAD
    ;;
  *)
    printf 'usage: %s status|commit-push\n' "$0" >&2
    exit 2
    ;;
esac
