#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md INSPIRATION.md scripts/inspect-query-engine-backlog.sh scripts/review-rejected-t042-parallel-replay.sh scripts/commit-rejected-t042-parallel-replay.sh scripts/push-rejected-t042-parallel-replay.sh Makefile
git commit -m "docs: record rejected parallel replay experiment"
