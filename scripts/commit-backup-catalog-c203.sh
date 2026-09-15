#!/usr/bin/env bash
set -euo pipefail

allowed_paths=(
  Makefile
  BACKUP_MANIFEST_CATALOG.md
  hat/hatBackup/catalog.go
  hat/hatBackup/catalog_test.go
  hat/hatBackup/catalog_benchmark_test.go
  scripts/inspect-backup-c203.sh
  scripts/test-backup-catalog-c203.sh
  scripts/benchmark-backup-catalog-c203.sh
  scripts/race-backup-catalog-c203.sh
  scripts/vet-backup-catalog-c203.sh
  scripts/format-backup-catalog-c203.sh
  scripts/stage-backup-catalog-c203.sh
  scripts/commit-backup-catalog-c203.sh
  scripts/push-backup-catalog-c203.sh
)
required_paths=(
  BACKUP_MANIFEST_CATALOG.md
  hat/hatBackup/catalog.go
  hat/hatBackup/catalog_test.go
)

if git diff --cached --quiet; then
  printf 'no staged backup catalog changes\n' >&2
  exit 1
fi
git diff --cached --check

for path in "${required_paths[@]}"; do
  git diff --cached --name-only -- "$path" | grep -Fx "$path" > /dev/null || {
    printf 'required staged path is missing: %s\n' "$path" >&2
    exit 1
  }
done

staged_paths="$(git diff --cached --name-only)"
while IFS= read -r path; do
  [[ -n "$path" ]] || continue
  allowed=0
  for expected_path in "${allowed_paths[@]}"; do
    if [[ "$path" == "$expected_path" ]]; then
      allowed=1
      break
    fi
  done
  if (( allowed == 0 )); then
    printf 'refusing unexpected staged path: %s\n' "$path" >&2
    exit 1
  fi
done <<< "$staged_paths"

git commit -m 'Add durable backup manifest catalog'
