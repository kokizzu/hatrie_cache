#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  BENCHMARK.md \
  INSPIRATION.md \
  Makefile \
  T043_RECOVERY_SCALAR_ARENA.md \
  hat/hatCache/journal.go \
  hat/hatCache/journal_segments.go \
  hat/hatCache/journal_replay_batch.go \
  hat/hatCache/journal_replay_scalar_decode.go \
  hat/hatCache/t043_replay_scalar_decode_test.go \
  scripts/benchmark-t043-replay.sh \
  scripts/format-t043-replay.sh \
  scripts/race-t043-replay.sh \
  scripts/ship-t043-recovery-replay.sh \
  scripts/test-t043-journal.sh \
  scripts/test-t043-replay.sh \
  scripts/vet-t043-replay.sh
git diff --cached --check
git commit -m 'feat(recovery): arena-backed scalar journal replay [skip ci]'
git push -u origin codex/next-inspiration-round13
