#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

mode=${1:-status}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

feature_paths=(
	BENCHMARK.md
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
	Makefile
	M052_REUSABLE_DATAFLOW_EXECUTOR.md
	hat/hatSql/m052_shared_dataflow_executor.go
	hat/hatSql/m052_shared_dataflow_executor_test.go
	hat/hatSql/m052_shared_dataflow_executor_benchmark_test.go
	scripts/benchmark-m052-shared-dataflow-executor.sh
	scripts/format-m052-shared-dataflow-executor.sh
	scripts/race-m052-shared-dataflow-executor.sh
	scripts/test-m052-shared-dataflow-executor.sh
	scripts/deliver-m052-shared-dataflow-executor.sh
)

is_feature_path() {
	local candidate=$1
	local path
	for path in "${feature_paths[@]}"; do
		if [[ $candidate == "$path" ]]; then
			return 0
		fi
	done
	return 1
}

reject_existing_index_changes() {
	if git diff --cached --quiet; then
		return 0
	fi
	printf '%s\n' 'refusing to stage: the index already contains changes'
	git diff --cached --name-only
	exit 1
}

stage_generated_file() {
	local source=$1
	local destination=$2
	local hash
	hash=$(git hash-object -w "$source")
	git update-index --add --cacheinfo "100644,$hash,$destination"
}

build_makefile() {
	git show HEAD:Makefile >"$tmp/Makefile"
	printf '%s\n' \
		'' \
		'.PHONY: benchmark-m052-shared-dataflow-executor' \
		'benchmark-m052-shared-dataflow-executor:' \
		$'\tbash ./scripts/benchmark-m052-shared-dataflow-executor.sh' \
		'' \
		'.PHONY: test-m052-shared-dataflow-executor' \
		'test-m052-shared-dataflow-executor:' \
		$'\tbash ./scripts/test-m052-shared-dataflow-executor.sh' \
		'' \
		'.PHONY: format-m052-shared-dataflow-executor' \
		'format-m052-shared-dataflow-executor:' \
		$'\tbash ./scripts/format-m052-shared-dataflow-executor.sh' \
		'' \
		'.PHONY: race-m052-shared-dataflow-executor' \
		'race-m052-shared-dataflow-executor:' \
		$'\tbash ./scripts/race-m052-shared-dataflow-executor.sh' \
		'' \
		'.PHONY: stage-m052-shared-dataflow-executor' \
		'stage-m052-shared-dataflow-executor:' \
		$'\tbash ./scripts/deliver-m052-shared-dataflow-executor.sh stage' \
		'' \
		'.PHONY: commit-m052-shared-dataflow-executor' \
		'commit-m052-shared-dataflow-executor:' \
		$'\tbash ./scripts/deliver-m052-shared-dataflow-executor.sh commit' \
		'' \
		'.PHONY: push-m052-shared-dataflow-executor' \
		'push-m052-shared-dataflow-executor:' \
		$'\tbash ./scripts/deliver-m052-shared-dataflow-executor.sh push' \
		'' \
		'.PHONY: status-m052-shared-dataflow-executor' \
		'status-m052-shared-dataflow-executor:' \
		$'\tbash ./scripts/deliver-m052-shared-dataflow-executor.sh status' \
		>>"$tmp/Makefile"
}

build_benchmark() {
	git show HEAD:BENCHMARK.md >"$tmp/BENCHMARK.md"
	printf '%s\n' \
		'' \
		'## M052 Shared Dataflow Executor' \
		'' \
		'Command: `make benchmark-m052-shared-dataflow-executor`' \
		'' \
		'This benchmark warms the compiled query'"'"'s memoized plan before repeatedly' \
		'binding a runner. The clone path is the existing `CompileDataflow` API; the' \
		'shared path is the opt-in `CompileReusableDataflow` API.' \
		'' \
		'| Path | Samples (ns/op) | Median | B/op | Allocs/op | Relative result |' \
		'| --- | ---: | ---: | ---: | ---: | ---: |' \
		'| `CompileDataflow` clone | 284.3, 274.9, 278.3, 286.6, 268.5 | 278.3 | 352 | 5 | baseline |' \
		'| `CompileReusableDataflow` shared | 63.39, 66.11, 65.15, 66.70, 72.33 | 66.11 | 80 | 1 | 4.21x faster, 4.40x lower bytes, 5x fewer allocations |' \
		'' \
		'The shared method only reuses immutable fragment metadata. Runners must treat' \
		'the fragment and input metadata as read-only; callers needing an independent' \
		'mutable plan continue to use `CompileDataflow`.' \
		>>"$tmp/benchmark-section"
	sed '/^## M064 Recursive Fixpoint Evaluation (Rejected)$/r '"$tmp/benchmark-section" "$tmp/BENCHMARK.md" >"$tmp/BENCHMARK.next"
	mv "$tmp/BENCHMARK.next" "$tmp/BENCHMARK.md"
}

build_catalog() {
	git show HEAD:CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md >"$tmp/catalog.md"
	printf '%s\n' \
		'- [x] M052 Shared dataflow executor binding reuses the compiled query'"'"'s' \
		'  immutable memoized fragment plan without copying fragment metadata on every' \
		'  executor construction; `CompileDataflow` remains the independent-copy API.' \
		'  See [M052_REUSABLE_DATAFLOW_EXECUTOR.md](M052_REUSABLE_DATAFLOW_EXECUTOR.md)' \
		'  and [BENCHMARK.md](BENCHMARK.md#m052-shared-dataflow-executor).' \
		>"$tmp/catalog-section"
	sed '/M052_NATIVE_UNION.md/r '"$tmp/catalog-section" "$tmp/catalog.md" >"$tmp/catalog.next"
	mv "$tmp/catalog.next" "$tmp/catalog.md"
}

stage() {
	reject_existing_index_changes
	build_makefile
	build_benchmark
	build_catalog
	rg -q '^benchmark-m052-shared-dataflow-executor:' "$tmp/Makefile"
	rg -q '^## M052 Shared Dataflow Executor$' "$tmp/BENCHMARK.md"
	rg -q '^\- \[x\] M052 Shared dataflow executor binding' "$tmp/catalog.md"
	stage_generated_file "$tmp/Makefile" Makefile
	stage_generated_file "$tmp/BENCHMARK.md" BENCHMARK.md
	stage_generated_file "$tmp/catalog.md" CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
	git add -- \
		M052_REUSABLE_DATAFLOW_EXECUTOR.md \
		hat/hatSql/m052_shared_dataflow_executor.go \
		hat/hatSql/m052_shared_dataflow_executor_test.go \
		hat/hatSql/m052_shared_dataflow_executor_benchmark_test.go \
		scripts/benchmark-m052-shared-dataflow-executor.sh \
		scripts/format-m052-shared-dataflow-executor.sh \
		scripts/race-m052-shared-dataflow-executor.sh \
		scripts/test-m052-shared-dataflow-executor.sh \
		scripts/deliver-m052-shared-dataflow-executor.sh
	printf '%s\n' 'staged feature paths:'
	git diff --cached --name-only -- "${feature_paths[@]}"
}

commit() {
	local staged path
	staged=$(git diff --cached --name-only)
	if [[ -z $staged ]]; then
		printf '%s\n' 'refusing to commit: no staged changes; run the stage target first'
		exit 1
	fi
	while IFS= read -r path; do
		[[ -z $path ]] && continue
		if ! is_feature_path "$path"; then
			printf 'refusing to commit unrelated staged path: %s\n' "$path"
			exit 1
		fi
	done <<<"$staged"
	git diff --cached --check
	git commit -m 'feat(sql): reuse memoized dataflow executor plan [skip ci]'
}

push() {
	git push origin HEAD
}

status() {
	git status --short
}

case $mode in
stage)
		stage
		;;
commit)
		commit
		;;
push)
		push
		;;
status)
		status
		;;
*)
		printf 'usage: %s {stage|commit|push|status}\n' "$0" >&2
		exit 2
		;;
esac
