#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  T042_RECOVERY_PARALLEL_REPLAY.md \
  Makefile \
  hat/hatJournal/parallel_replay.go \
  hat/hatJournal/parallel_replay_test.go \
  hat/hatJournal/parallel_replay_benchmark_test.go \
  scripts/benchmark-round34-journal.sh \
  scripts/commit-round34-journal.sh \
  scripts/format-round34-journal.sh \
  scripts/push-round34-journal.sh \
  scripts/race-round34-journal.sh \
  scripts/review-round34-journal.sh \
  scripts/stage-round34-journal.sh \
  scripts/test-round34-journal.sh \
  scripts/verify-round34-journal-docs.sh \
  scripts/vet-round34-journal.sh
