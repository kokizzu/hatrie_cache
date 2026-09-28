#!/usr/bin/env bash
set -euo pipefail

repo_root="$(pwd)"
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-low-cardinality-delivery.XXXXXX")"
temporary_worktree="$temporary_root/worktree"

feature_paths=(
	LOW_CARDINALITY_STRING_CODEC.md
	hat/hatCodec/low_cardinality_dictionary.go
	hat/hatCodec/low_cardinality_dictionary_test.go
	hat/hatCodec/low_cardinality_dictionary_benchmark_test.go
	scripts/benchmark-codec-low-cardinality.sh
	scripts/deliver-low-cardinality.sh
	scripts/format-codec-low-cardinality.sh
	scripts/race-codec-low-cardinality.sh
	scripts/test-codec-low-cardinality-package.sh
	scripts/test-codec-low-cardinality.sh
)

cleanup() {
	git -C "$repo_root" worktree remove --force "$temporary_worktree" >/dev/null 2>&1 || true
	rm -rf "$temporary_root"
}
trap cleanup EXIT

git -C "$repo_root" fetch origin master
base_revision="$(git -C "$repo_root" rev-parse origin/master)"
git -C "$repo_root" worktree add --detach "$temporary_worktree" "$base_revision" >/dev/null

for path in "${feature_paths[@]}"; do
	destination="$temporary_worktree/$path"
	mkdir -p "$(dirname "$destination")"
	cp "$repo_root/$path" "$destination"
done

makefile="$temporary_worktree/Makefile"
marker="# low-cardinality-string-feature"
if ! grep -Fq "$marker" "$makefile"; then
	printf '%s\n' \
		"$marker" \
		'benchmark-codec-low-cardinality:' \
		$'\tbash scripts/benchmark-codec-low-cardinality.sh' \
		'format-codec-low-cardinality:' \
		$'\tbash scripts/format-codec-low-cardinality.sh' \
		'test-codec-low-cardinality:' \
		$'\tbash scripts/test-codec-low-cardinality.sh' \
		'test-codec-low-cardinality-package:' \
		$'\tbash scripts/test-codec-low-cardinality-package.sh' \
		'race-codec-low-cardinality:' \
		$'\tbash scripts/race-codec-low-cardinality.sh' \
		'deliver-codec-low-cardinality:' \
		$'\tbash scripts/deliver-low-cardinality.sh' \
		>>"$makefile"
fi

git -C "$temporary_worktree" add -- Makefile "${feature_paths[@]}"
staged_paths="$(git -C "$temporary_worktree" diff --cached --name-only)"
while IFS= read -r path; do
	case "$path" in
	Makefile)
		;;
	*)
		allowed=false
		for feature_path in "${feature_paths[@]}"; do
			if [[ "$path" == "$feature_path" ]]; then
				allowed=true
				break
			fi
		done
		if [[ "$allowed" != true ]]; then
			printf 'unexpected staged path: %s\n' "$path" >&2
			exit 1
		fi
		;;
	esac
done <<<"$staged_paths"
printf '%s\n' 'staged paths:'
git -C "$temporary_worktree" diff --cached --name-status

git -C "$temporary_worktree" commit -m 'feat(codec): add low-cardinality string blocks [skip ci]'
commit_hash="$(git -C "$temporary_worktree" rev-parse HEAD)"
git -C "$temporary_worktree" push origin HEAD:master
printf 'pushed commit: %s\n' "$commit_hash"
