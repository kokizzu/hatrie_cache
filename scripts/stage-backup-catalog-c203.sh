#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf 'refusing to stage backup catalog with pre-existing staged changes\n' >&2
  exit 1
fi

head_makefile="$(mktemp)"
clean_makefile="$(mktemp)"
trap 'rm -f -- "$head_makefile" "$clean_makefile"' EXIT

git show HEAD:Makefile > "$head_makefile"
cp "$head_makefile" "$clean_makefile"
if ! grep -Fq 'test-backup-catalog-c203:' "$head_makefile"; then
  printf '\n.PHONY: inspect-backup-c203 test-backup-catalog-c203 benchmark-backup-catalog-c203 race-backup-catalog-c203 vet-backup-catalog-c203 format-backup-catalog-c203\ninspect-backup-c203:\n\tbash ./scripts/inspect-backup-c203.sh\n\ntest-backup-catalog-c203:\n\tbash ./scripts/test-backup-catalog-c203.sh\n\nbenchmark-backup-catalog-c203:\n\tbash ./scripts/benchmark-backup-catalog-c203.sh\n\nrace-backup-catalog-c203:\n\tbash ./scripts/race-backup-catalog-c203.sh\n\nvet-backup-catalog-c203:\n\tbash ./scripts/vet-backup-catalog-c203.sh\n\nformat-backup-catalog-c203:\n\tbash ./scripts/format-backup-catalog-c203.sh\n\n.PHONY: stage-backup-catalog-c203 commit-backup-catalog-c203 push-backup-catalog-c203\nstage-backup-catalog-c203:\n\tbash ./scripts/stage-backup-catalog-c203.sh\n\ncommit-backup-catalog-c203:\n\tbash ./scripts/commit-backup-catalog-c203.sh\n\npush-backup-catalog-c203:\n\tbash ./scripts/push-backup-catalog-c203.sh\n' >> "$clean_makefile"
fi

makefile_blob="$(git hash-object -w "$clean_makefile")"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git add -- BACKUP_MANIFEST_CATALOG.md hat/hatBackup/catalog.go hat/hatBackup/catalog_test.go hat/hatBackup/catalog_benchmark_test.go scripts/inspect-backup-c203.sh scripts/test-backup-catalog-c203.sh scripts/benchmark-backup-catalog-c203.sh scripts/race-backup-catalog-c203.sh scripts/vet-backup-catalog-c203.sh scripts/format-backup-catalog-c203.sh scripts/stage-backup-catalog-c203.sh scripts/commit-backup-catalog-c203.sh scripts/push-backup-catalog-c203.sh
git diff --cached --check
git diff --cached --stat
git diff --cached --name-only
