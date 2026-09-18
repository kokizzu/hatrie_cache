#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  README.md \
  scripts/cleanup-hatrie-tmp.sh \
  scripts/review-cleanup-hatrie-tmp.sh \
  scripts/stage-cleanup-hatrie-tmp.sh \
  scripts/test-cleanup-hatrie-tmp.sh
git diff --cached --check
git diff --cached --name-status
