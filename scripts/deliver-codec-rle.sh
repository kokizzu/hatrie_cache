#!/usr/bin/env bash
set -euo pipefail

repo_root="$(pwd)"
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-rle-deliver.XXXXXX")"
temporary_worktree="$temporary_root/worktree"

cleanup() {
	git -C "$repo_root" worktree remove --force "$temporary_worktree" >/dev/null 2>&1 || true
	rm -rf "$temporary_root"
}
trap cleanup EXIT

git -C "$repo_root" fetch origin master
git -C "$repo_root" worktree add --detach "$temporary_worktree" origin/master >/dev/null

paths=(
	"RUN_LENGTH_UINT64_CODEC.md"
	"hat/hatCodec/run_length_uint64.go"
	"hat/hatCodec/run_length_uint64_test.go"
	"hat/hatCodec/run_length_uint64_benchmark_test.go"
	"scripts/benchmark-codec-rle.sh"
	"scripts/format-codec-rle.sh"
	"scripts/test-codec-rle.sh"
	"scripts/test-codec-rle-package.sh"
	"scripts/race-codec-rle.sh"
	"scripts/deliver-codec-rle.sh"
)

for path in "${paths[@]}"; do
	mkdir -p "$temporary_worktree/$(dirname "$path")"
	cp "$repo_root/$path" "$temporary_worktree/$path"
done

if ! grep -q '^benchmark-codec-rle:' "$temporary_worktree/Makefile"; then
	printf '%s\n' \
		'' \
		'benchmark-codec-rle:' \
		$'\tbash scripts/benchmark-codec-rle.sh' \
		'' \
		'format-codec-rle:' \
		$'\tbash scripts/format-codec-rle.sh' \
		'' \
		'test-codec-rle:' \
		$'\tbash scripts/test-codec-rle.sh' \
		'' \
		'test-codec-rle-package:' \
		$'\tbash scripts/test-codec-rle-package.sh' \
		'' \
		'race-codec-rle:' \
		$'\tbash scripts/race-codec-rle.sh' \
		'' \
		'deliver-codec-rle:' \
		$'\tbash scripts/deliver-codec-rle.sh' \
		>> "$temporary_worktree/Makefile"
fi

git -C "$temporary_worktree" add -- \
		Makefile \
		RUN_LENGTH_UINT64_CODEC.md \
		hat/hatCodec/run_length_uint64.go \
		hat/hatCodec/run_length_uint64_test.go \
		hat/hatCodec/run_length_uint64_benchmark_test.go \
		scripts/benchmark-codec-rle.sh \
		scripts/format-codec-rle.sh \
		scripts/test-codec-rle.sh \
		scripts/test-codec-rle-package.sh \
		scripts/race-codec-rle.sh \
		scripts/deliver-codec-rle.sh
git -C "$temporary_worktree" diff --cached --check
git -C "$temporary_worktree" commit -m 'feat(codec): add run-length uint64 blocks [skip ci]'
git -C "$temporary_worktree" push origin HEAD:master
