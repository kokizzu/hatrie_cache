#!/usr/bin/env bash
set -euo pipefail

repo_root="$(pwd)"
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-default-suppression-delivery.XXXXXX")"
temporary_worktree="$temporary_root/worktree"

feature_paths=(
	DEFAULT_VALUE_SUPPRESSION.md
	hat/hatCodec/default_value_suppression.go
	hat/hatCodec/default_value_suppression_test.go
	hat/hatCodec/default_value_suppression_benchmark_test.go
	scripts/benchmark-codec-default-value-suppression.sh
	scripts/format-codec-default-value-suppression.sh
	scripts/race-codec-default-value-suppression.sh
	scripts/test-codec-default-value-suppression-package.sh
	scripts/test-codec-default-value-suppression.sh
	scripts/deliver-default-value-suppression.sh
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
marker="# default-value-suppression-feature"
if ! grep -Fq "$marker" "$makefile"; then
	printf '%s\n' \
		"$marker" \
		'benchmark-codec-default-value-suppression:' \
		$'\tbash scripts/benchmark-codec-default-value-suppression.sh' \
		'test-codec-default-value-suppression:' \
		$'\tbash scripts/test-codec-default-value-suppression.sh' \
		'format-codec-default-value-suppression:' \
		$'\tbash scripts/format-codec-default-value-suppression.sh' \
		'test-codec-default-value-suppression-package:' \
		$'\tbash scripts/test-codec-default-value-suppression-package.sh' \
		'race-codec-default-value-suppression:' \
		$'\tbash scripts/race-codec-default-value-suppression.sh' \
		'deliver-codec-default-value-suppression:' \
		$'\tbash scripts/deliver-default-value-suppression.sh' \
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

git -C "$temporary_worktree" commit -m 'feat(codec): add default-value suppression [skip ci]'
commit_hash="$(git -C "$temporary_worktree" rev-parse HEAD)"
git -C "$temporary_worktree" push origin HEAD:master
printf 'pushed commit: %s\n' "$commit_hash"
