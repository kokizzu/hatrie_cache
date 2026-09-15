#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf 'refusing to stage cleanup feature with pre-existing staged changes\n' >&2
  exit 1
fi

head_makefile="$(mktemp)"
clean_makefile="$(mktemp)"
trap 'rm -f -- "$head_makefile" "$clean_makefile"' EXIT

git show HEAD:Makefile > "$head_makefile"
cp "$head_makefile" "$clean_makefile"
printf '\n.PHONY: cleanup-test-tmp-preview cleanup-test-tmp-apply\ncleanup-test-tmp-preview:\n\tbash ./scripts/cleanup-test-tmp.sh preview\n\ncleanup-test-tmp-apply:\n\tbash ./scripts/cleanup-test-tmp.sh apply\n\n.PHONY: test-cleanup-test-tmp\ntest-cleanup-test-tmp:\n\tbash ./scripts/test-cleanup-test-tmp.sh\n\n.PHONY: stage-cleanup-test-tmp commit-cleanup-test-tmp push-cleanup-test-tmp\nstage-cleanup-test-tmp:\n\tbash ./scripts/stage-cleanup-test-tmp.sh\n\ncommit-cleanup-test-tmp:\n\tbash ./scripts/commit-cleanup-test-tmp.sh\n\npush-cleanup-test-tmp:\n\tbash ./scripts/push-cleanup-test-tmp.sh\n' >> "$clean_makefile"

makefile_blob="$(git hash-object -w "$clean_makefile")"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git add -- scripts/cleanup-test-tmp.sh scripts/test-cleanup-test-tmp.sh scripts/stage-cleanup-test-tmp.sh scripts/commit-cleanup-test-tmp.sh scripts/push-cleanup-test-tmp.sh
git diff --cached --check
git diff --cached --stat
git diff --cached --name-only
