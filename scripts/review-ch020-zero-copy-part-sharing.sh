#!/usr/bin/env bash
set -euo pipefail

expected_paths=(
	BENCHMARK.md
	ENGINE_IDEAS.md
	Makefile
	CH020_ZERO_COPY_PART_SHARING.md
	hat/hatMerkle/ch020_zero_copy_part_sharing.go
	hat/hatMerkle/ch020_zero_copy_part_sharing_benchmark_test.go
	hat/hatMerkle/ch020_zero_copy_part_sharing_test.go
	scripts/ch020-zero-copy-part-sharing.sh
	scripts/stage-ch020-zero-copy-part-sharing.sh
	scripts/review-ch020-zero-copy-part-sharing.sh
	scripts/commit-ch020-zero-copy-part-sharing.sh
	scripts/push-ch020-zero-copy-part-sharing.sh
)

staged_paths="$(git diff --cached --name-only)"
if [ -z "$staged_paths" ]; then
	printf '%s\n' 'no staged paths' >&2
	exit 1
fi
for path in $staged_paths; do
	allowed=false
	for expected in "${expected_paths[@]}"; do
		if [ "$path" = "$expected" ]; then
			allowed=true
			break
		fi
	done
	if [ "$allowed" != true ]; then
		printf 'unexpected staged path: %s\n' "$path" >&2
		exit 1
	fi
done

git diff --cached --check
git diff --cached --name-status
git diff --cached --stat
