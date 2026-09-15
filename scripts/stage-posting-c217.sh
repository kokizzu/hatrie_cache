#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT

git show HEAD:Makefile > "$base_makefile"
{
	printf '\n'
	printf '%s\n' \
		'test-posting-c217:' \
		$'\t@bash ./scripts/test-posting-c217.sh' \
		'' \
		'benchmark-posting-c217:' \
		$'\t@bash ./scripts/benchmark-posting-c217.sh' \
		'' \
		'benchmark-hash-index-c217:' \
		$'\t@bash ./scripts/benchmark-hash-index-c217.sh' \
		'' \
		'format-posting-c217:' \
		$'\t@bash ./scripts/format-posting-c217.sh' \
		'' \
		'verify-posting-c217:' \
		$'\t@bash ./scripts/verify-posting-c217.sh' \
		'' \
		'inspect-posting-c217:' \
		$'\t@bash ./scripts/inspect-posting-c217.sh' \
		'' \
		'stage-posting-c217:' \
		$'\t@bash ./scripts/stage-posting-c217.sh' \
		'' \
		'commit-posting-c217:' \
		$'\t@bash ./scripts/commit-posting-c217.sh' \
		'' \
		'push-posting-c217:' \
		$'\t@bash ./scripts/push-posting-c217.sh'
} >> "$base_makefile"

git add -- BENCHMARK.md INSPIRATION.md \
	hat/hatDataStructure/compact_posting.go \
	hat/hatDataStructure/compact_posting_benchmark_test.go \
	hat/hatDataStructure/compact_posting_test.go \
	scripts/benchmark-hash-index-c217.sh \
	scripts/benchmark-posting-c217.sh \
	scripts/commit-posting-c217.sh \
	scripts/format-posting-c217.sh \
	scripts/inspect-posting-c217.sh \
	scripts/push-posting-c217.sh \
	scripts/stage-posting-c217.sh \
	scripts/test-posting-c217.sh \
	scripts/verify-posting-c217.sh
makefile_blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
