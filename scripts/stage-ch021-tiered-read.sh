#!/usr/bin/env bash
set -euo pipefail

git add \
  ENGINE_IDEAS.md \
  CH021_OBJECT_STORAGE_TIERING.md \
  Makefile \
  hat/hatStorage/ch021_tiered_reader.go \
  hat/hatStorage/ch021_tiered_read_test.go \
  hat/hatStorage/ch021_tiered_read_baseline_benchmark_test.go \
  scripts/benchmark-ch021-tiered-read.sh \
  scripts/commit-ch021-tiered-read.sh \
  scripts/format-ch021-tiered-read.sh \
  scripts/push-ch021-tiered-read.sh \
  scripts/race-ch021-tiered-read.sh \
  scripts/stage-ch021-tiered-read.sh \
  scripts/test-ch021-package.sh \
  scripts/test-ch021-tiered-read.sh \
  scripts/vet-ch021-tiered-read.sh
git diff --cached --check
git diff --cached --stat
