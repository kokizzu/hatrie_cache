#!/usr/bin/env bash
set -euo pipefail

repo_root="$(pwd)"
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-bitmap-deliver.XXXXXX")"
temporary_worktree="$temporary_root/worktree"

cleanup() {
	git -C "$repo_root" worktree remove --force "$temporary_worktree" >/dev/null 2>&1 || true
	rm -rf "$temporary_root"
}
trap cleanup EXIT

git -C "$repo_root" fetch origin master
git -C "$repo_root" worktree add --detach "$temporary_worktree" origin/master >/dev/null

paths=(
	"BOOL_BITMAP_CODEC.md"
	"hat/hatCodec/bool_bitmap.go"
	"hat/hatCodec/bool_bitmap_test.go"
	"hat/hatCodec/bool_bitmap_benchmark_test.go"
	"scripts/benchmark-codec-bitmap.sh"
	"scripts/format-codec-bitmap.sh"
	"scripts/test-codec-bitmap.sh"
	"scripts/test-codec-bitmap-package.sh"
	"scripts/race-codec-bitmap.sh"
	"scripts/deliver-codec-bitmap.sh"
)

for path in "${paths[@]}"; do
	mkdir -p "$temporary_worktree/$(dirname "$path")"
	cp "$repo_root/$path" "$temporary_worktree/$path"
done

if ! grep -q '^benchmark-codec-bitmap:' "$temporary_worktree/Makefile"; then
	printf '%s\n' \
		'' \
		'benchmark-codec-bitmap:' \
		$'\tbash scripts/benchmark-codec-bitmap.sh' \
		'' \
		'format-codec-bitmap:' \
		$'\tbash scripts/format-codec-bitmap.sh' \
		'' \
		'test-codec-bitmap:' \
		$'\tbash scripts/test-codec-bitmap.sh' \
		'' \
		'test-codec-bitmap-package:' \
		$'\tbash scripts/test-codec-bitmap-package.sh' \
		'' \
		'race-codec-bitmap:' \
		$'\tbash scripts/race-codec-bitmap.sh' \
		'' \
		'deliver-codec-bitmap:' \
		$'\tbash scripts/deliver-codec-bitmap.sh' \
		>> "$temporary_worktree/Makefile"
fi

(cd "$temporary_worktree" && make test-codec-bitmap-package)
(cd "$temporary_worktree" && make race-codec-bitmap)

git -C "$temporary_worktree" add -- \
		Makefile \
		BOOL_BITMAP_CODEC.md \
		hat/hatCodec/bool_bitmap.go \
		hat/hatCodec/bool_bitmap_test.go \
		hat/hatCodec/bool_bitmap_benchmark_test.go \
		scripts/benchmark-codec-bitmap.sh \
		scripts/format-codec-bitmap.sh \
		scripts/test-codec-bitmap.sh \
		scripts/test-codec-bitmap-package.sh \
		scripts/race-codec-bitmap.sh \
		scripts/deliver-codec-bitmap.sh
git -C "$temporary_worktree" diff --cached --check
git -C "$temporary_worktree" commit -m 'feat(codec): add boolean bitmap blocks [skip ci]'
git -C "$temporary_worktree" push origin HEAD:master
