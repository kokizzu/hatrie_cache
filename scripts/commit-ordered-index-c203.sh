#!/usr/bin/env bash
set -euo pipefail

paths=(
	BENCHMARK.md
	INSPIRATION_BACKLOG.md
	Makefile
	README.md
	TR052_ORDERED_INDEX_SMALL_VECTOR.md
	hat/hatDataStructure/ordered_index.go
	hat/hatDataStructure/ordered_index_benchmark_test.go
	hat/hatDataStructure/ordered_index_test.go
	scripts/benchmark-ordered-index-c203.sh
	scripts/commit-ordered-index-c203.sh
	scripts/inspect-staged-ordered-index-c203.sh
	scripts/push-ordered-index-c203.sh
	scripts/stage-ordered-index-c203.sh
	scripts/test-ordered-index-c203.sh
	scripts/verify-ordered-index-c203.sh
)

if git diff --cached --quiet; then
	echo "refusing to commit: no staged changes" >&2
	exit 1
fi
git diff --cached --check
declare -A expected_paths=()
for path in "${paths[@]}"; do
	expected_paths["$path"]=1
done
actual_count=0
while IFS= read -r path; do
	if [[ -z "$path" ]]; then
		continue
	fi
	if [[ -z "${expected_paths[$path]+present}" ]]; then
		echo "refusing to commit unexpected staged path: $path" >&2
		exit 1
	fi
	actual_count=$((actual_count + 1))
done < <(git diff --cached --name-only)
if ((actual_count != ${#paths[@]})); then
	echo "refusing to commit: staged path count does not match the feature" >&2
	exit 1
fi
git commit -m "Optimize small ordered indexes"
