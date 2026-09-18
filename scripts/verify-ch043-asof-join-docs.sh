#!/usr/bin/env bash
set -eu

test -s CH043_ASOF_TEMPORAL_JOIN.md
grep -q 'CH-43' CH043_ASOF_TEMPORAL_JOIN.md
grep -q 'AsOfJoin' README.md
grep -q 'CH-43: ASOF temporal join' BENCHMARK.md
grep -q 'CH-43.*\[x\]' INSPIRATION_BACKLOG.md
