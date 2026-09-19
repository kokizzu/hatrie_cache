#!/usr/bin/env bash
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
current_head=$(git rev-parse HEAD)
git fetch origin master
base=$(git rev-parse origin/master)
stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cleanup-stage.XXXXXX")
archive=$(mktemp "${TMPDIR:-/tmp}/hatrie-cleanup-archive.XXXXXX.tar")
index=$(mktemp "${TMPDIR:-/tmp}/hatrie-cleanup-index.XXXXXX")
trap 'rm -rf "$stage" "$archive" "$index"' EXIT

git archive --format=tar --output="$archive" "$base"
tar -xf "$archive" -C "$stage"
mkdir -p "$stage/scripts"
git show "$base:scripts/cleanup-test-tmp.sh" > "$stage/scripts/cleanup-test-tmp.sh"
cp "$repo/scripts/commit-cleanup-tooling.sh" "$stage/scripts/commit-cleanup-tooling.sh"
cp "$repo/scripts/push-cleanup-tooling.sh" "$stage/scripts/push-cleanup-tooling.sh"

cat >> "$stage/Makefile" <<'EOF'

# BEGIN safe test temporary cleanup
.PHONY: cleanup-test-tmp-preview cleanup-test-tmp cleanup-hatrie-tmp-audit-preview cleanup-hatrie-tmp commit-cleanup-tooling push-cleanup-tooling
cleanup-test-tmp-preview:
	bash ./scripts/cleanup-test-tmp.sh preview
cleanup-test-tmp:
	bash ./scripts/cleanup-test-tmp.sh apply
cleanup-hatrie-tmp-audit-preview:
	bash ./scripts/cleanup-test-tmp.sh preview
cleanup-hatrie-tmp:
	bash ./scripts/cleanup-test-tmp.sh apply
commit-cleanup-tooling:
	bash ./scripts/commit-cleanup-tooling.sh
push-cleanup-tooling:
	bash ./scripts/push-cleanup-tooling.sh
# END safe test temporary cleanup
EOF

rm -f "$index"
export GIT_INDEX_FILE="$index"
git read-tree "$base"
git --work-tree="$stage" add -- Makefile scripts/cleanup-test-tmp.sh scripts/commit-cleanup-tooling.sh scripts/push-cleanup-tooling.sh
tree=$(git write-tree)
commit=$(git commit-tree "$tree" -p "$base" -m "chore: add safe test temporary cleanup")
git update-ref HEAD "$commit" "$current_head"
printf 'Cleanup tooling isolated commit: %s\n' "$commit"
git diff-tree --stat --oneline "$commit"
