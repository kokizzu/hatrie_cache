#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/verify-t224-scope.sh
git add BENCHMARK.md INSPIRATION_ROUND2.md Makefile README.md T224_PARTIAL_INDEX.md \
  scripts/benchmark-t224.sh scripts/commit-t224.sh scripts/format-t224.sh \
  scripts/push-t224.sh scripts/race-t224.sh scripts/stage-t224.sh \
  scripts/test-t224.sh scripts/verify-t224-scope.sh scripts/vet-t224.sh
git diff --cached --check
git status --short
