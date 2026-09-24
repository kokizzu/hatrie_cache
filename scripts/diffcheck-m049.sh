#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat

for file in \
  scripts/inspect-m049-gaps.sh \
  scripts/inspect-m049-consolidation.sh \
  scripts/inspect-m049-quorum.sh \
  scripts/inspect-m049-candidates.sh \
  scripts/inspect-m049-quorum-path.sh \
  scripts/inspect-m049-sql-surface.sh \
  scripts/inspect-m049-plan-cache.sh \
  scripts/inspect-m049-status.sh \
  scripts/inspect-m049-idea-headings.sh \
  scripts/inspect-m049-singleflight.sh \
  scripts/inspect-m049-doc-tail.sh \
  scripts/show-m049-makefile-head.sh; do
  if test -e "$file"; then
    printf 'temporary inspection file remains: %s\n' "$file" >&2
    exit 1
  fi
done
