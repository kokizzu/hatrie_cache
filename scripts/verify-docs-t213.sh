#!/usr/bin/env bash
set -euo pipefail

test -f T213_SCHEDULED_SNAPSHOTS.md
rg -q 'T213 Scheduled Snapshots With Checkpoint Manifests' BENCHMARK.md
rg -q 'T213 Scheduled snapshots with checkpoint manifests and atomic publication' INSPIRATION_ROUND2.md
rg -q 'T213_SCHEDULED_SNAPSHOTS.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md
