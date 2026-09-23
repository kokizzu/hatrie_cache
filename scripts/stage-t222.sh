#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/verify-t222-scope.sh
git add BENCHMARK.md INSPIRATION_ROUND2.md Makefile README.md T222_MULTIKEY_INDEX.md \
  scripts/benchmark-t222.sh scripts/commit-t222.sh scripts/format-t222.sh \
  scripts/push-t222.sh scripts/race-t222.sh scripts/stage-t222.sh \
  scripts/test-t222.sh scripts/verify-t222-scope.sh scripts/vet-t222.sh
git diff --cached --check
git status --short
