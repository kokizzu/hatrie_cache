#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  scripts/audit-hatrie-tmp.sh \
  scripts/audit-hatrie-tmp-deep.sh \
  scripts/cleanup-hatrie-tmp-safe.sh \
  scripts/remove-local-hatrie-plan.sh \
  scripts/test-hatrie-tmp-cleanup.sh \
  scripts/stage-hatrie-tmp-cleanup.sh \
  scripts/commit-hatrie-tmp-cleanup.sh \
  scripts/push-hatrie-tmp-cleanup.sh \
  scripts/review-hatrie-tmp-cleanup.sh
git diff --cached --check
git diff --cached --stat
