#!/usr/bin/env bash
set -euo pipefail

repo=$(pwd)
git_dir="$repo/.git"
git -C "$repo" fetch origin master >/dev/null
base=$(git -C "$repo" rev-parse origin/master)
current_head=$(git -C "$repo" rev-parse HEAD)

if [[ "$current_head" != "$base" ]]; then
	printf 'refusing isolated commit: HEAD %s is not origin/master %s\n' "$current_head" "$base" >&2
	exit 1
fi

stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m050-baseline-stage.XXXXXX")
archive=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-baseline-archive.XXXXXX.tar")
index=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-baseline-index.XXXXXX")
message=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-baseline-message.XXXXXX")
cleanup() {
	rm -rf "$stage" "$archive" "$index" "$message"
}
trap cleanup EXIT

git -C "$repo" archive --format=tar --output="$archive" "$base"
tar -xf "$archive" -C "$stage"

cat >>"$stage/Makefile" <<'EOF'

.PHONY: commit-m050-baseline-fix push-m050-baseline-fix
commit-m050-baseline-fix:
	bash ./scripts/commit-m050-baseline-fix.sh
push-m050-baseline-fix:
	bash ./scripts/push-m050-baseline-fix.sh
EOF

feature_paths=(
	Makefile
	scripts/benchmark-m050-baseline.sh
	scripts/commit-m050-baseline-fix.sh
	scripts/push-m050-baseline-fix.sh
)
for path in "${feature_paths[@]}"; do
	if [[ ! -f "$repo/$path" && ! -f "$stage/$path" ]]; then
		printf 'missing feature path: %s\n' "$path" >&2
		exit 1
	fi
done
for path in "${feature_paths[@]}"; do
	if [[ "$path" != "Makefile" ]]; then
		mkdir -p "$stage/$(dirname "$path")"
		cp "$repo/$path" "$stage/$path"
	fi
done

GIT_INDEX_FILE="$index" git -C "$repo" read-tree "$base"
GIT_INDEX_FILE="$index" git --git-dir="$git_dir" --work-tree="$stage" add -- "${feature_paths[@]}"
tree=$(GIT_INDEX_FILE="$index" git -C "$repo" write-tree)
printf 'fix: keep M050 baseline pinned to its feature parent\n\nLocate the M-U50 commit by subject so later maintenance commits do not move the clean baseline.\n' >"$message"
commit=$(GIT_INDEX_FILE="$index" git -C "$repo" commit-tree "$tree" -p "$base" <"$message")
git -C "$repo" update-ref HEAD "$commit" "$current_head"
printf '%s\n' "$commit"

