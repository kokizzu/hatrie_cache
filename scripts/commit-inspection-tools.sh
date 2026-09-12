#!/bin/sh
set -eu

git add Makefile \
  scripts/inspect-open-inspiration-local-clean.sh \
  scripts/inspect-files.sh \
  scripts/search-files.sh \
  scripts/inspect-timestamp-ordering.sh \
  scripts/review-inspection-tools.sh \
  scripts/commit-inspection-tools.sh \
  scripts/push-inspection-tools.sh
git diff --cached --check
git commit -m "chore: add makefile-backed inspection helpers"
