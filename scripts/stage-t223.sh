#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/verify-t223-scope.sh
git add BENCHMARK.md INSPIRATION_ROUND2.md Makefile README.md T223_FUNCTIONAL_INDEX.md \
  scripts/benchmark-t223.sh scripts/commit-t223.sh scripts/format-t223.sh \
  scripts/push-t223.sh scripts/race-t223.sh scripts/stage-t223.sh \
  scripts/test-t223.sh scripts/verify-t223-scope.sh scripts/vet-t223.sh
git diff --cached --check
git status --short
