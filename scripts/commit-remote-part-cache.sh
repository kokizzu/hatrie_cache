#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md REMOTE_PART_CACHE.md hat/hatStorage/remote_part_cache.go hat/hatStorage/remote_part_cache_test.go hat/hatStorage/remote_part_cache_baseline_test.go scripts/benchmark-remote-part-cache-after.sh scripts/benchmark-remote-part-cache-before.sh scripts/check-remote-part-cache.sh scripts/commit-remote-part-cache.sh scripts/format-remote-part-cache.sh scripts/push-remote-part-cache.sh scripts/race-remote-part-cache.sh scripts/review-inspiration-next.sh scripts/review-remote-part-cache.sh scripts/test-remote-part-cache-all.sh scripts/test-remote-part-cache-before.sh scripts/test-remote-part-cache-repo.sh scripts/test-remote-part-cache.sh scripts/vet-remote-part-cache.sh
git diff --cached --check
git diff --cached --stat
git commit -m "feat: add bounded remote-part cache"
