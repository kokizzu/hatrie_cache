#!/usr/bin/env bash
set -euo pipefail

test -s MZ028_TEMPORAL_INTERVAL_ARRANGEMENT.md
rg -q 'MZ028_TEMPORAL_INTERVAL_ARRANGEMENT.md' README.md
rg -q 'SQLTemporalIntervalArrangement' MZ028_TEMPORAL_INTERVAL_ARRANGEMENT.md
rg -q '## MZ-028: Temporal interval arrangement' BENCHMARK.md
rg -q 'MZ-28.*\[x\]' INSPIRATION_BACKLOG.md
printf '%s\n' 'MZ-028 documentation links and benchmark markers verified.'
