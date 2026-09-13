#!/usr/bin/env bash
set -euo pipefail

expected_parent='40751ad98895d7bc6aa381b7b237fbdeb661a234'
commit_message='feat(hatPipeline): adapt async batch flush to arrival rate'
current_parent=$(git rev-parse HEAD)
if [[ "$current_parent" != "$expected_parent" ]]; then
	printf 'refusing C201 commit: expected parent %s, found %s\n' "$expected_parent" "$current_parent" >&2
	exit 1
fi

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing C201 commit: the index already contains staged changes' >&2
	exit 1
fi

declare -a allowed_paths=(
	Makefile
	README.md
	INSPIRATION_ROUND2.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	C201_ADAPTIVE_ASYNC_BATCHER.md
	hat/hatPipeline/async_batcher.go
	hat/hatPipeline/c201_adaptive_batcher_test.go
	scripts/benchmark-c201.sh
	scripts/format-c201.sh
	scripts/test-c201.sh
	scripts/test-c201-package.sh
	scripts/test-c201-race.sh
	scripts/vet-c201.sh
	scripts/commit-c201.sh
	scripts/push-c201.sh
)

declare -A allowed=()
for path in "${allowed_paths[@]}"; do
	allowed["$path"]=1
done

while IFS= read -r status_line; do
	path=${status_line:3}
	if [[ "$path" == *' -> '* ]]; then
		path=${path##* -> }
	fi
	if [[ -z "${allowed[$path]:-}" ]]; then
		printf 'refusing C201 commit: unexpected worktree path %s\n' "$path" >&2
		exit 1
	fi
done < <(git status --porcelain=v1 --untracked-files=all)

git add -- "${allowed_paths[@]}"
if git diff --cached --quiet; then
	printf '%s\n' 'refusing C201 commit: no C201 changes are available' >&2
	exit 1
fi

git commit -m "$commit_message"
