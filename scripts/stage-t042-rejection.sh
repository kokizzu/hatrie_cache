#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  T042_PARALLEL_REPLAY_REJECTION.md \
  scripts/commit-t042-rejection.sh \
  scripts/push-t042-rejection.sh \
  scripts/stage-t042-rejection.sh \
  scripts/verify-t042-rejection.sh
git diff --cached --check
git diff --cached --stat
