#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT

git show HEAD:Makefile > "$base_makefile"
{
	printf '\n'
	printf '%s\n' \
		'inspect-index-c216:' \
		$'\t@bash ./scripts/inspect-index-c216.sh' \
		'' \
		'test-index-c216:' \
		$'\t@bash ./scripts/test-index-c216.sh' \
		'' \
		'benchmark-index-c216:' \
		$'\t@bash ./scripts/benchmark-index-c216.sh' \
		'' \
		'benchmark-index-lookup-c216:' \
		$'\t@bash ./scripts/benchmark-index-lookup-c216.sh' \
		'' \
		'benchmark-index-posting-c216:' \
		$'\t@bash ./scripts/benchmark-index-posting-c216.sh' \
		'' \
		'format-index-c216:' \
		$'\t@bash ./scripts/format-index-c216.sh' \
		'' \
		'verify-index-c216:' \
		$'\t@bash ./scripts/verify-index-c216.sh' \
		'' \
		'stage-index-c216:' \
		$'\t@bash ./scripts/stage-index-c216.sh' \
		'' \
		'commit-index-c216:' \
		$'\t@bash ./scripts/commit-index-c216.sh' \
		'' \
		'push-index-c216:' \
		$'\t@bash ./scripts/push-index-c216.sh'
} >> "$base_makefile"

git add -- BENCHMARK.md INSPIRATION.md \
	hat/hatDataStructure/compact_posting.go \
	hat/hatDataStructure/compact_posting_benchmark_test.go \
	hat/hatDataStructure/compact_posting_test.go \
	scripts/benchmark-index-c216.sh \
	scripts/benchmark-index-lookup-c216.sh \
	scripts/benchmark-index-posting-c216.sh \
	scripts/format-index-c216.sh \
	scripts/inspect-index-c216.sh \
	scripts/commit-index-c216.sh \
	scripts/push-index-c216.sh \
	scripts/stage-index-c216.sh \
	scripts/test-index-c216.sh \
	scripts/verify-index-c216.sh
makefile_blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
