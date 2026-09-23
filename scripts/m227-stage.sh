#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  IDEA_GAP_CATALOG.md \
  TTG08_PARALLEL_REPLAY_REJECTED.md \
  Makefile \
  scripts/m227-commit.sh \
  scripts/m227-landscape.sh \
  scripts/m227-replay-context.sh \
  scripts/m227-stage.sh \
  scripts/m227-verify-catalog.sh
git diff --cached --check
git diff --cached --stat
