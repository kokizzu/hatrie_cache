#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/stage-t115.sh

allowed_files=(
  Makefile
  INSPIRATION.md
  README.md
  hat/hatCache/replication.go
  hat/hatCache/replication_task_ownership_test.go
  scripts/format-t115.sh
  scripts/test-race-t115.sh
  scripts/test-t115.sh
  scripts/vet-t115.sh
  scripts/review-t115.sh
  scripts/stage-t115.sh
  scripts/commit-t115.sh
  scripts/push-t115.sh
)

while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  found=false
  for allowed in "${allowed_files[@]}"; do
    if [[ "$path" == "$allowed" ]]; then
      found=true
      break
    fi
  done
  if [[ "$found" != true ]]; then
    printf 'refusing to commit unexpected staged path: %s\n' "$path" >&2
    exit 1
  fi
done < <(git diff --cached --name-only)

if git diff --cached --quiet; then
  printf '%s\n' 'T115 has no staged changes.'
  exit 0
fi

git commit -m 'feat: make async replication shutdown cancellation-safe'
