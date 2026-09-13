#!/usr/bin/env bash
set -euo pipefail

rg -n 'REMOTE_PART_PREFETCH.md' README.md
rg -n 'RemotePartCache\.Prefetch|RemotePartPrefetchOptions|TestRemotePartCachePrefetch' hat/hatStorage --glob '*.go'
rg -n 'CH-016 Remote-Part Prefetch|BenchmarkRemotePartCachePrefetch' BENCHMARK.md
rg -n '^\| CH-16 \|.*\[x\]' INSPIRATION_BACKLOG.md
