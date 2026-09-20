#!/usr/bin/env bash
set -euo pipefail

git diff --cached -- \
  Makefile \
  scripts/audit-hatrie-tmp.sh \
  scripts/cleanup-hatrie-tmp-safe.sh \
  scripts/test-hatrie-tmp-cleanup.sh \
  scripts/stage-hatrie-tmp-cleanup.sh \
  scripts/commit-hatrie-tmp-cleanup.sh \
  scripts/push-hatrie-tmp-cleanup.sh \
  scripts/review-hatrie-tmp-cleanup.sh
