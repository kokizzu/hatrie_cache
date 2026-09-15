#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT

git show HEAD:Makefile > "$base_makefile"
{
	printf '\n'
	printf '%s\n' \
		'test-rtree-c219:' \
		$'\t@bash ./scripts/test-rtree-c219.sh' \
		'' \
		'benchmark-rtree-c219:' \
		$'\t@bash ./scripts/benchmark-rtree-c219.sh' \
		'' \
		'format-rtree-c219:' \
		$'\t@bash ./scripts/format-rtree-c219.sh' \
		'' \
		'verify-rtree-c219:' \
		$'\t@bash ./scripts/verify-rtree-c219.sh' \
		'' \
		'inspect-rtree-c219:' \
		$'\t@bash ./scripts/inspect-rtree-c219.sh' \
		'' \
		'stage-rtree-c219:' \
		$'\t@bash ./scripts/stage-rtree-c219.sh' \
		'' \
		'commit-rtree-c219:' \
		$'\t@bash ./scripts/commit-rtree-c219.sh' \
		'' \
		'push-rtree-c219:' \
		$'\t@bash ./scripts/push-rtree-c219.sh'
} >> "$base_makefile"

git add -- BENCHMARK.md INSPIRATION.md \
	hat/hatDataStructure/rtree.go \
	hat/hatDataStructure/rtree_search_fastpath_test.go \
	hat/hatDataStructure/rtree_search_fastpath_benchmark_test.go \
	scripts/benchmark-rtree-c219.sh \
	scripts/commit-rtree-c219.sh \
	scripts/format-rtree-c219.sh \
	scripts/inspect-rtree-c219.sh \
	scripts/push-rtree-c219.sh \
	scripts/stage-rtree-c219.sh \
	scripts/test-rtree-c219.sh \
	scripts/verify-rtree-c219.sh
makefile_blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
