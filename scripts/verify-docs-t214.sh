#!/usr/bin/env bash
set -euo pipefail

test -f T214_STREAMING_SNAPSHOTS.md
rg -q 'T214 Streaming Snapshots Without a Shared Filesystem' BENCHMARK.md
rg -q 'T214 Streaming snapshots for replicas without a shared filesystem' INSPIRATION_ROUND2.md
rg -q 'T214_STREAMING_SNAPSHOTS.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md
