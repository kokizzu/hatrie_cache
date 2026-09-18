#!/usr/bin/env bash
set -euo pipefail

test -f MZ020_TWO_PHASE_SINK_CHECKPOINT.md
printf '%s\n' 'verified MZ020_TWO_PHASE_SINK_CHECKPOINT.md exists'
if ! rg -q 'MZ020_TWO_PHASE_SINK_CHECKPOINT.md' README.md INSPIRATION_BACKLOG.md; then
  printf '%s\n' 'missing README/backlog link'
  exit 1
fi
printf '%s\n' 'verified README/backlog link'
if ! rg -q '# MZ-020: Two-Phase Sink Progress Checkpoints' MZ020_TWO_PHASE_SINK_CHECKPOINT.md; then
  printf '%s\n' 'missing MZ-020 document heading'
  exit 1
fi
printf '%s\n' 'verified MZ-020 document heading'
if ! rg -q 'Existing one-phase baseline' BENCHMARK.md; then
  printf '%s\n' 'missing baseline benchmark'
  exit 1
fi
printf '%s\n' 'verified baseline benchmark'
if ! rg -q 'Two-phase coordinator' BENCHMARK.md; then
  printf '%s\n' 'missing two-phase benchmark'
  exit 1
fi
printf '%s\n' 'verified two-phase benchmark'
