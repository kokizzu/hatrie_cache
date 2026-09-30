#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  BENCHMARK.md \
  INSPIRATION.md \
  Makefile \
  T042_RECOVERY_REPLAY_BATCH.md \
  hat/hatCache/journal.go \
  hat/hatCache/journal_replay_batch.go \
  hat/hatCache/t042_recovery_replay_batch_test.go \
  scripts/benchmark-t042-recovery-replay.sh \
  scripts/race-t042-recovery-replay.sh \
  scripts/ship-t042-recovery-replay.sh \
  scripts/test-t042-recovery-backup.sh \
  scripts/test-t042-recovery-replay-coverage.sh \
  scripts/test-t042-recovery-replay.sh \
  scripts/vet-t042-recovery-replay.sh
git diff --cached --check
git commit -m 'feat(recovery): batch scalar journal replay [skip ci]'
git push -u origin codex/next-inspiration-round12-base
