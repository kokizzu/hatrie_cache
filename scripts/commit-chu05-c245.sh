#!/usr/bin/env bash
set -euo pipefail

allowlist=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CHU05_EXTERNAL_WINDOW_STREAM.md
	Makefile
	PRODUCT_IDEA_GAPS.md
	README.md
	hat/hatSql/chu05_external_window_stream_benchmark_test.go
	hat/hatSql/chu05_external_window_stream_test.go
	hat/hatSql/query.go
	scripts/benchmark-chu05-c245.sh
	scripts/commit-chu05-c245.sh
	scripts/format-chu05-c245.sh
	scripts/memory-chu05-c245.sh
	scripts/push-chu05-c245.sh
	scripts/race-chu05-c245.sh
	scripts/stage-chu05-c245.sh
	scripts/test-chu05-c245.sh
	scripts/test-chu05-package-c245.sh
	scripts/verify-chu05-docs-c245.sh
	scripts/vet-chu05-c245.sh
)

is_allowed() {
	local candidate="$1"
	local allowed
	for allowed in "${allowlist[@]}"; do
		if [[ "$candidate" == "$allowed" ]]; then
			return 0
		fi
	done
	return 1
}

staged_paths="$(git diff --cached --name-only)"
staged_count=0
while IFS= read -r path; do
	if [[ -n "$path" ]]; then
		staged_count=$((staged_count + 1))
		if ! is_allowed "$path"; then
			printf 'Refusing commit: unrelated path is staged: %s\n' "$path" >&2
			exit 1
		fi
	fi
done <<< "$staged_paths"
if [[ "$staged_count" -ne "${#allowlist[@]}" ]]; then
	printf 'Refusing commit: staged %s paths, expected %s CH-U05 paths.\n' "$staged_count" "${#allowlist[@]}" >&2
	exit 1
fi
git diff --cached --check
git commit -m 'feat: stream external windows [skip ci]'
