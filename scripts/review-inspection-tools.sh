#!/bin/sh
set -eu

git diff --check
git status --short
git diff --stat -- Makefile scripts/inspect-open-inspiration-local-clean.sh scripts/inspect-files.sh scripts/search-files.sh scripts/inspect-timestamp-ordering.sh scripts/review-inspection-tools.sh scripts/commit-inspection-tools.sh scripts/push-inspection-tools.sh
