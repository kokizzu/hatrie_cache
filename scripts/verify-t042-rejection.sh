#!/usr/bin/env bash
set -euo pipefail

test -s T042_RECOVERY_PARALLEL_REPLAY_EVALUATION.md
rg -n 'rejected and rolled back|8,320|14,445,445|6.35|removed' T042_RECOVERY_PARALLEL_REPLAY_EVALUATION.md BENCHMARK.md
