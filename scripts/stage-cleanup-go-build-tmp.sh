#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  scripts/commit-cleanup-go-build-tmp.sh \
  scripts/push-cleanup-go-build-tmp.sh \
  scripts/stage-cleanup-go-build-tmp.sh

git diff --cached --check
git diff --cached --stat
