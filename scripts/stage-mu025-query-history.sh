#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile PRODUCT_IDEA_GAPS.md \
  scripts/commit-mu025-query-history.sh \
  scripts/push-mu025-query-history.sh \
  scripts/review-mu025-query-history.sh \
  scripts/stage-mu025-query-history.sh \
  scripts/verify-mu025-query-history.sh
git diff --cached --check
git diff --cached --stat
git status --short
