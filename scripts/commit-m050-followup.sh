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

stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m050-followup-stage.XXXXXX")
archive=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-followup-archive.XXXXXX.tar")
index=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-followup-index.XXXXXX")
message=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-followup-message.XXXXXX")
cleanup() {
	rm -rf "$stage" "$archive" "$index" "$message"
}
trap cleanup EXIT

git -C "$repo" archive --format=tar --output="$archive" "$base"
tar -xf "$archive" -C "$stage"

if rg -q '^cleanup-hatrie-build-tmp-preview:' "$stage/Makefile"; then
	printf 'build artifact cleanup targets already exist in base\n' >&2
	exit 1
fi

cat >>"$stage/Makefile" <<'EOF'

.PHONY: audit-hatrie-build-tmp
audit-hatrie-build-tmp:
	bash ./scripts/audit-hatrie-build-tmp.sh

.PHONY: cleanup-hatrie-build-tmp-preview cleanup-hatrie-build-tmp cleanup-hatrie-build-tmp-verify
cleanup-hatrie-build-tmp-preview:
	bash ./scripts/cleanup-hatrie-build-tmp.sh preview
cleanup-hatrie-build-tmp:
	bash ./scripts/cleanup-hatrie-build-tmp.sh apply
cleanup-hatrie-build-tmp-verify:
	bash ./scripts/cleanup-hatrie-build-tmp.sh verify

.PHONY: commit-m050-followup push-m050-followup
commit-m050-followup:
	bash ./scripts/commit-m050-followup.sh
push-m050-followup:
	bash ./scripts/push-m050-followup.sh
EOF

feature_paths=(
	Makefile
	scripts/benchmark-m050-baseline.sh
	scripts/audit-hatrie-build-tmp.sh
	scripts/cleanup-hatrie-build-tmp.sh
	scripts/commit-m050-followup.sh
	scripts/push-m050-followup.sh
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
printf 'chore: make M050 baseline reproducible and clean stale build artifacts\n\nUse HEAD^ by default for the pre-feature control benchmark and add reviewed, allowlisted cleanup for stale Hatrie build/test artifacts under /tmp.\n' >"$message"
commit=$(GIT_INDEX_FILE="$index" git -C "$repo" commit-tree "$tree" -p "$base" <"$message")
git -C "$repo" update-ref HEAD "$commit" "$current_head"
printf '%s\n' "$commit"

