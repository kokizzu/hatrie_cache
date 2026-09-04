#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to stage T046: the index already contains changes' >&2
  exit 1
fi

git add -- \
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

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
git show :Makefile > "$tmpdir/base-makefile"
cp "$tmpdir/base-makefile" "$tmpdir/t046-makefile"
printf '%s\n' \
  '.PHONY: test-t046' \
  'test-t046:' \
  $'\tbash ./scripts/test-t046.sh' \
  '.PHONY: format-t046' \
  'format-t046:' \
  $'\tbash ./scripts/format-t046.sh' \
  '.PHONY: test-race-t046' \
  'test-race-t046:' \
  $'\tbash ./scripts/test-race-t046.sh' \
  '.PHONY: vet-t046' \
  'vet-t046:' \
  $'\tbash ./scripts/vet-t046.sh' \
  '.PHONY: review-t046' \
  'review-t046:' \
  $'\tbash ./scripts/review-t046.sh' \
  '.PHONY: stage-t046' \
  'stage-t046:' \
  $'\tbash ./scripts/stage-t046.sh' \
  '.PHONY: commit-t046' \
  'commit-t046:' \
  $'\tbash ./scripts/commit-t046.sh' \
  '.PHONY: push-t046' \
  'push-t046:' \
  $'\tbash ./scripts/push-t046.sh' >> "$tmpdir/t046-makefile"
set +e
git diff --no-index --no-ext-diff "$tmpdir/base-makefile" "$tmpdir/t046-makefile" > "$tmpdir/raw.patch"
diff_status=$?
set -e
if [ "$diff_status" -gt 1 ]; then
  exit "$diff_status"
fi
sed -e '1c diff --git a/Makefile b/Makefile' \
  -e '3c --- a/Makefile' \
  -e '4c +++ b/Makefile' \
  "$tmpdir/raw.patch" > "$tmpdir/makefile.patch"
git apply --cached "$tmpdir/makefile.patch"
