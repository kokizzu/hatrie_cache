#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf 'Refusing to commit: the index already contains staged changes.\n' >&2
  exit 1
fi

git add -- scripts/audit-tmp-hatrie-all.sh scripts/commit-tmp-cleanup.sh scripts/push-tmp-cleanup.sh

patch=$(mktemp /tmp/hatrie-tmp-cleanup-index.XXXXXX)
trap 'rm -f "$patch"' EXIT
tab=$'\t'
printf '%s\n' \
  'diff --git a/Makefile b/Makefile' \
  '--- a/Makefile' \
  '+++ b/Makefile' \
  '@@ -0,0 +1,16 @@' \
  '+.PHONY: audit-tmp-hatrie-all' \
  '+audit-tmp-hatrie-all:' \
  "+${tab}bash ./scripts/audit-tmp-hatrie-all.sh \$(or \$(MODE),preview)" \
  '+' \
  '+.PHONY: cleanup-tmp-hatrie-all' \
  '+cleanup-tmp-hatrie-all:' \
  "+${tab}bash ./scripts/audit-tmp-hatrie-all.sh apply" \
  '+' \
  '+.PHONY: commit-tmp-cleanup' \
  '+commit-tmp-cleanup:' \
  "+${tab}bash ./scripts/commit-tmp-cleanup.sh" \
  '+' \
  '+.PHONY: push-tmp-cleanup' \
  '+push-tmp-cleanup:' \
  "+${tab}bash ./scripts/push-tmp-cleanup.sh" \
  '+' > "$patch"
git apply --cached --unidiff-zero "$patch"

git diff --cached --check
mapfile -t staged < <(git diff --cached --name-only)
if (( ${#staged[@]} != 4 )); then
  printf 'Unexpected staged file count: %s\n' "${#staged[@]}" >&2
  exit 1
fi
for expected in Makefile scripts/audit-tmp-hatrie-all.sh scripts/commit-tmp-cleanup.sh scripts/push-tmp-cleanup.sh; do
  if ! printf '%s\n' "${staged[@]}" | grep -Fxq "$expected"; then
    printf 'Unexpected staged set; missing %s.\n' "$expected" >&2
    exit 1
  fi
done

git commit -m 'chore: add tmp cleanup workflow'
