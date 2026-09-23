#!/usr/bin/env bash
set -euo pipefail

git commit --only -m 'chore: track M227 push helper' -- \
  BENCHMARK.md \
  IDEA_GAP_CATALOG.md \
  TTG08_PARALLEL_REPLAY_REJECTED.md \
  Makefile \
  scripts/m227-commit.sh \
  scripts/m227-landscape.sh \
  scripts/m227-push.sh \
  scripts/m227-replay-context.sh \
  scripts/m227-stage.sh \
  scripts/m227-verify-catalog.sh \
  scripts/m227-verify-rollback.sh
