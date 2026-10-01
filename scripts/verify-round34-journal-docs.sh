#!/usr/bin/env bash
set -euo pipefail

test -s T042_RECOVERY_PARALLEL_REPLAY.md
rg -q 'T042_RECOVERY_PARALLEL_REPLAY.md' README.md
rg -q '3.10x faster' BENCHMARK.md
rg -q 'ParallelReplay' PRODUCT_IDEA_GAPS.md
