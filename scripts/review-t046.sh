#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat -- \
  INSPIRATION.md \
  README.md \
  hat/hatCache/backup_bundle.go \
  hat/hatCache/backup_context.go \
  hat/hatCache/backup_context_test.go \
  hat/hatCache/backup_repository.go \
  scripts/format-t046.sh \
  scripts/review-t046.sh \
  scripts/stage-t046.sh \
  scripts/commit-t046.sh \
  scripts/push-t046.sh \
  scripts/test-t046.sh \
  scripts/test-race-t046.sh \
  scripts/vet-t046.sh
printf '%s\n' 'staged paths:'
git diff --cached --name-status
printf '%s\n' 'staged Makefile tail:'
git diff --cached -- Makefile | tail -80
printf '%s\n' 'unstaged Makefile tail:'
git diff -- Makefile | tail -80
git status --short
