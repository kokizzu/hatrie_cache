#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT

git show HEAD:Makefile > "$base_makefile"
{
	printf '\n'
	printf '%s\n' \
		'test-posting-remove-c218:' \
		$'\t@bash ./scripts/test-posting-remove-c218.sh' \
		'' \
		'benchmark-posting-remove-c218:' \
		$'\t@bash ./scripts/benchmark-posting-remove-c218.sh' \
		'' \
		'format-posting-remove-c218:' \
		$'\t@bash ./scripts/format-posting-remove-c218.sh' \
		'' \
		'verify-posting-remove-c218:' \
		$'\t@bash ./scripts/verify-posting-remove-c218.sh' \
		'' \
		'inspect-posting-remove-c218:' \
		$'\t@bash ./scripts/inspect-posting-remove-c218.sh' \
		'' \
		'stage-posting-remove-c218:' \
		$'\t@bash ./scripts/stage-posting-remove-c218.sh' \
		'' \
		'commit-posting-remove-c218:' \
		$'\t@bash ./scripts/commit-posting-remove-c218.sh' \
		'' \
		'push-posting-remove-c218:' \
		$'\t@bash ./scripts/push-posting-remove-c218.sh'
} >> "$base_makefile"

git add -- BENCHMARK.md INSPIRATION.md \
	hat/hatDataStructure/compact_posting.go \
	hat/hatDataStructure/compact_posting_benchmark_test.go \
	hat/hatDataStructure/compact_posting_test.go \
	scripts/benchmark-posting-remove-c218.sh \
	scripts/commit-posting-remove-c218.sh \
	scripts/format-posting-remove-c218.sh \
	scripts/inspect-posting-remove-c218.sh \
	scripts/push-posting-remove-c218.sh \
	scripts/stage-posting-remove-c218.sh \
	scripts/test-posting-remove-c218.sh \
	scripts/verify-posting-remove-c218.sh
makefile_blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
