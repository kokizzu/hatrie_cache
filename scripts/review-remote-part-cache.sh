#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --stat
grep -n -E 'REMOTE_PART_CACHE|CH-008|RemotePartCache|BenchmarkRemotePartCache' README.md BENCHMARK.md INSPIRATION_BACKLOG.md REMOTE_PART_CACHE.md hat/hatStorage/remote_part_cache.go hat/hatStorage/remote_part_cache_test.go
