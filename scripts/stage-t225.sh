#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/verify-t225-scope.sh
git add BENCHMARK.md INSPIRATION_ROUND2.md Makefile README.md T225_COVERING_INDEX.md \
  scripts/benchmark-t225.sh scripts/commit-t225.sh scripts/format-t225.sh \
  scripts/push-t225.sh scripts/race-t225.sh scripts/stage-t225.sh \
  scripts/test-t225.sh scripts/verify-t225-scope.sh scripts/vet-t225.sh
git diff --cached --check
git status --short
