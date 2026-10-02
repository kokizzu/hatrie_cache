#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CH026_MERGE_SELECTOR.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatStorage/ch026_merge_selector_benchmark_test.go \
  hat/hatStorage/ch026_merge_selector_test.go \
  hat/hatStorage/compaction_merge_selector.go \
  scripts/commit-round20-ch026-merge-selector.sh \
  scripts/push-round20-ch026-merge-selector.sh \
  scripts/run-ch026-merge-selector-checks.sh \
  scripts/stage-round20-ch026-merge-selector.sh
git diff --cached --check
git status --short
