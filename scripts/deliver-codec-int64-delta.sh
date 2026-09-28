#!/usr/bin/env bash
set -euo pipefail

repo_root="$(pwd)"
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-int64-delta-deliver.XXXXXX")"
temporary_worktree="$temporary_root/worktree"

cleanup() {
	git -C "$repo_root" worktree remove --force "$temporary_worktree" >/dev/null 2>&1 || true
	rm -rf "$temporary_root"
}
trap cleanup EXIT

git -C "$repo_root" fetch origin master
git -C "$repo_root" worktree add --detach "$temporary_worktree" origin/master >/dev/null

paths=(
	"INT64_DELTA_CODEC.md"
	"hat/hatCodec/int64_delta.go"
	"hat/hatCodec/int64_delta_test.go"
	"hat/hatCodec/int64_delta_benchmark_test.go"
	"scripts/benchmark-codec-int64-delta.sh"
	"scripts/format-codec-int64-delta.sh"
	"scripts/test-codec-int64-delta.sh"
	"scripts/test-codec-int64-delta-package.sh"
	"scripts/race-codec-int64-delta.sh"
	"scripts/deliver-codec-int64-delta.sh"
)

for path in "${paths[@]}"; do
	mkdir -p "$temporary_worktree/$(dirname "$path")"
	cp "$repo_root/$path" "$temporary_worktree/$path"
done

if ! grep -q '^benchmark-codec-int64-delta:' "$temporary_worktree/Makefile"; then
	printf '%s\n' \
		'' \
		'benchmark-codec-int64-delta:' \
		$'\tbash scripts/benchmark-codec-int64-delta.sh' \
		'' \
		'format-codec-int64-delta:' \
		$'\tbash scripts/format-codec-int64-delta.sh' \
		'' \
		'test-codec-int64-delta:' \
		$'\tbash scripts/test-codec-int64-delta.sh' \
		'' \
		'test-codec-int64-delta-package:' \
		$'\tbash scripts/test-codec-int64-delta-package.sh' \
		'' \
		'race-codec-int64-delta:' \
		$'\tbash scripts/race-codec-int64-delta.sh' \
		'' \
		'deliver-codec-int64-delta:' \
		$'\tbash scripts/deliver-codec-int64-delta.sh' \
		>> "$temporary_worktree/Makefile"
fi

(cd "$temporary_worktree" && make test-codec-int64-delta-package)
(cd "$temporary_worktree" && make race-codec-int64-delta)

git -C "$temporary_worktree" add -- \
		Makefile \
		INT64_DELTA_CODEC.md \
		hat/hatCodec/int64_delta.go \
		hat/hatCodec/int64_delta_test.go \
		hat/hatCodec/int64_delta_benchmark_test.go \
		scripts/benchmark-codec-int64-delta.sh \
		scripts/format-codec-int64-delta.sh \
		scripts/test-codec-int64-delta.sh \
		scripts/test-codec-int64-delta-package.sh \
		scripts/race-codec-int64-delta.sh \
		scripts/deliver-codec-int64-delta.sh
git -C "$temporary_worktree" diff --cached --check
git -C "$temporary_worktree" commit -m 'feat(codec): add int64 delta blocks [skip ci]'
git -C "$temporary_worktree" push origin HEAD:master
