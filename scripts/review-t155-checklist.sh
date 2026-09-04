#!/usr/bin/env bash
set -euo pipefail

files=(
  INSPIRATION.md
  scripts/commit-t155-checklist.sh
  scripts/push-t155-checklist.sh
  scripts/review-t155-checklist.sh
  scripts/stage-t155-checklist.sh
)

git diff --check -- "${files[@]}"
git diff --stat -- "${files[@]}"
git status --short
