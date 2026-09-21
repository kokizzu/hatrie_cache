#!/usr/bin/env bash
set -euo pipefail

test -f T042_PARALLEL_REPLAY_REJECTION.md
rg -n 'Status: rejected|Raw measurements|1\.89x|4\.34x|implementation was removed' T042_PARALLEL_REPLAY_REJECTION.md

if rg -n 'ReplayParallel|CommandJournalParallelReplay' hat/hatCache --glob '*.go'; then
  printf '%s\n' 'T042 runtime implementation still exists' >&2
  exit 1
fi

for path in \
  hat/hatCache/t042_parallel_replay.go \
  hat/hatCache/t042_parallel_replay_test.go \
  hat/hatCache/t042_parallel_replay_benchmark_test.go \
  scripts/t042-recovery-parallel.sh \
  scripts/inspect-inspiration-state.sh \
  scripts/inspect-candidate.sh \
  scripts/inspect-generated-columns.sh \
  scripts/inspect-gap-ledgers.sh \
  scripts/inspect-t042-replay.sh \
  scripts/show-t042-benchmark.sh; do
  if test -e "$path"; then
    printf 'temporary T042 path still exists: %s\n' "$path" >&2
    exit 1
  fi
done

printf '%s\n' 'T042 rejection report verified; no runtime implementation remains.'
