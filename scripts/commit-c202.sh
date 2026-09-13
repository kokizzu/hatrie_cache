#!/usr/bin/env bash
set -euo pipefail

expected_parent='5606982daf5cfc1dcb8f402efe948273679f5fff'
commit_message='feat(hatPipeline): add partition-affine async batching'
current_parent=$(git rev-parse HEAD)
if [[ "$current_parent" != "$expected_parent" ]]; then
	printf 'refusing C202 commit: expected parent %s, found %s\n' "$expected_parent" "$current_parent" >&2
	exit 1
fi

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing C202 commit: the index already contains staged changes' >&2
	exit 1
fi

git diff --check

declare -a allowed_paths=(
	Makefile
	README.md
	INSPIRATION_ROUND2.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	C202_PARTITIONED_ASYNC_BATCHER.md
	hat/hatPipeline/partitioned_async_batcher.go
	hat/hatPipeline/c202_partitioned_async_batcher_test.go
	hat/hatPipeline/c202_partitioned_async_batcher_benchmark_test.go
	scripts/benchmark-c202.sh
	scripts/benchmark-c202-serial.sh
	scripts/benchmark-c202-setup.sh
	scripts/format-c202.sh
	scripts/test-c202.sh
	scripts/test-c202-package.sh
	scripts/race-c202.sh
	scripts/vet-c202.sh
	scripts/commit-c202.sh
	scripts/push-c202.sh
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
		printf 'refusing C202 commit: unexpected worktree path %s\n' "$path" >&2
		exit 1
	fi
done < <(git status --porcelain=v1 --untracked-files=all)

git add -- "${allowed_paths[@]}"
if git diff --cached --quiet; then
	printf '%s\n' 'refusing C202 commit: no C202 changes are available' >&2
	exit 1
fi

git commit -m "$commit_message"
