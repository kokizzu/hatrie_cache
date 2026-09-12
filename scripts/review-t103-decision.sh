#!/bin/sh
set -eu

git status --short --branch
git diff --check -- Makefile INSPIRATION.md scripts/review-t103-decision.sh scripts/commit-t103-decision.sh scripts/push-t103-decision.sh
git diff --stat -- Makefile INSPIRATION.md scripts/review-t103-decision.sh scripts/commit-t103-decision.sh scripts/push-t103-decision.sh
