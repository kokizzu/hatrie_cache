#!/usr/bin/env bash
set -euo pipefail

repo_root="$(pwd)"
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-float-xor-deliver.XXXXXX")"
temporary_worktree="$temporary_root/worktree"

cleanup() {
	git -C "$repo_root" worktree remove --force "$temporary_worktree" >/dev/null 2>&1 || true
	rm -rf "$temporary_root"
}
trap cleanup EXIT

git -C "$repo_root" fetch origin master
git -C "$repo_root" worktree add --detach "$temporary_worktree" origin/master >/dev/null

paths=(
	"FLOAT64_XOR_CODEC.md"
	"hat/hatCodec/float64_xor.go"
	"hat/hatCodec/float64_xor_test.go"
	"hat/hatCodec/float64_xor_benchmark_test.go"
	"scripts/benchmark-codec-float-xor.sh"
	"scripts/format-codec-float-xor.sh"
	"scripts/test-codec-float-xor.sh"
	"scripts/test-codec-float-xor-package.sh"
	"scripts/race-codec-float-xor.sh"
	"scripts/deliver-codec-float-xor.sh"
)

for path in "${paths[@]}"; do
	mkdir -p "$temporary_worktree/$(dirname "$path")"
	cp "$repo_root/$path" "$temporary_worktree/$path"
done

if ! grep -q '^benchmark-codec-float-xor:' "$temporary_worktree/Makefile"; then
	printf '%s\n' \
		'' \
		'benchmark-codec-float-xor:' \
		$'\tbash scripts/benchmark-codec-float-xor.sh' \
		'' \
		'format-codec-float-xor:' \
		$'\tbash scripts/format-codec-float-xor.sh' \
		'' \
		'test-codec-float-xor:' \
		$'\tbash scripts/test-codec-float-xor.sh' \
		'' \
		'test-codec-float-xor-package:' \
		$'\tbash scripts/test-codec-float-xor-package.sh' \
		'' \
		'race-codec-float-xor:' \
		$'\tbash scripts/race-codec-float-xor.sh' \
		'' \
		'deliver-codec-float-xor:' \
		$'\tbash scripts/deliver-codec-float-xor.sh' \
		>> "$temporary_worktree/Makefile"
fi

(cd "$temporary_worktree" && make test-codec-float-xor-package)
(cd "$temporary_worktree" && make race-codec-float-xor)

git -C "$temporary_worktree" add -- \
		Makefile \
		FLOAT64_XOR_CODEC.md \
		hat/hatCodec/float64_xor.go \
		hat/hatCodec/float64_xor_test.go \
		hat/hatCodec/float64_xor_benchmark_test.go \
		scripts/benchmark-codec-float-xor.sh \
		scripts/format-codec-float-xor.sh \
		scripts/test-codec-float-xor.sh \
		scripts/test-codec-float-xor-package.sh \
		scripts/race-codec-float-xor.sh \
		scripts/deliver-codec-float-xor.sh
git -C "$temporary_worktree" diff --cached --check
git -C "$temporary_worktree" commit -m 'feat(codec): add float64 XOR blocks [skip ci]'
git -C "$temporary_worktree" push origin HEAD:master
