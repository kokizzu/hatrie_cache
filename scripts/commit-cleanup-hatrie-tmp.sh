#!/usr/bin/env bash
set -euo pipefail

makefile_tmp="$(mktemp /tmp/.hatrie-cleanup-makefile.XXXXXX)"
isolated_index="$(mktemp /tmp/.hatrie-cleanup-index.XXXXXX)"
trap 'rm -f -- "$makefile_tmp" "$isolated_index"' EXIT
rm -f -- "$isolated_index"
GIT_INDEX_FILE="$isolated_index" git read-tree HEAD

git show HEAD:Makefile > "$makefile_tmp"
if ! grep -q '^cleanup-hatrie-tmp-commit:' "$makefile_tmp"; then
  printf '\n%s\n' \
    '.PHONY: cleanup-hatrie-tmp-commit cleanup-hatrie-tmp-push' \
    'cleanup-hatrie-tmp-commit:' \
    $'\tbash scripts/commit-cleanup-hatrie-tmp.sh' \
    'cleanup-hatrie-tmp-push:' \
    $'\tbash scripts/push-cleanup-hatrie-tmp.sh' >> "$makefile_tmp"
fi

makefile_blob="$(git hash-object -w "$makefile_tmp")"
GIT_INDEX_FILE="$isolated_index" git update-index --add --cacheinfo 100644,"$makefile_blob",Makefile
GIT_INDEX_FILE="$isolated_index" git add -- scripts/cleanup-hatrie-tmp.sh scripts/commit-cleanup-hatrie-tmp.sh scripts/push-cleanup-hatrie-tmp.sh
GIT_INDEX_FILE="$isolated_index" git diff --cached --check

printf '%s\n' 'Staged cleanup feature paths:'
GIT_INDEX_FILE="$isolated_index" git diff --cached --name-only
GIT_INDEX_FILE="$isolated_index" git commit -m 'maint: add safe Hatrie tmp cleanup'
