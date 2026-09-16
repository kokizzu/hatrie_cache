#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet --; then
	printf '%s\n' 'Refusing to stage MZ-07: the index already contains changes.' >&2
	exit 1
fi

git add -- \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	MZ007_FRONTIER_SOURCE_BACKPRESSURE.md \
	README.md \
	hat/hatPipeline/mz007_frontier_backpressure.go \
	hat/hatPipeline/mz007_frontier_backpressure_benchmark_test.go \
	hat/hatPipeline/mz007_frontier_backpressure_test.go \
	scripts/benchmark-mz007-frontier-backpressure.sh \
	scripts/commit-mz007-frontier-backpressure.sh \
	scripts/format-mz007-frontier-backpressure.sh \
	scripts/race-mz007-frontier-backpressure.sh \
	scripts/review-mz007-frontier-backpressure.sh \
	scripts/stage-mz007-frontier-backpressure.sh \
	scripts/test-mz007-frontier-backpressure.sh \
	scripts/verify-mz007-frontier-backpressure.sh \
	scripts/vet-mz007-frontier-backpressure.sh

# The worktree contains unrelated concurrent edits to these tracked files.
# Stage clean HEAD-based copies with only this feature's additions.
adopted_tmp=$(mktemp)
adopted_base=$(mktemp)
makefile_tmp=$(mktemp)
trap 'rm -f "$adopted_tmp" "$adopted_base" "$makefile_tmp"' EXIT

git show HEAD:ADOPTED_QUERY_ENGINE_IDEAS.md > "$adopted_base"
code_tick=$(printf '\140')
adopted_row="| Materialize | Frontier-aware source backpressure | Adopted as an opt-in source admission gate | $code_tick"hatPipeline.FrontierBackpressure"$code_tick bounds producer progress in a caller-selected frontier domain, supports nonblocking rejection or cancellation-aware waiting, wakes on monotone consumer advancement and close, and retains no source records. Automatic connector wiring and distributed coordination remain caller-owned. See [MZ007_FRONTIER_SOURCE_BACKPRESSURE.md](MZ007_FRONTIER_SOURCE_BACKPRESSURE.md) and [BENCHMARK.md#mz-07-frontier-aware-source-backpressure](BENCHMARK.md#mz-07-frontier-aware-source-backpressure). |"
awk -v row="$adopted_row" '{ print; if ($0 ~ /^\| Materialize \| Stale-read rejection \|/) print row }' "$adopted_base" > "$adopted_tmp"
adopted_blob=$(git hash-object -w "$adopted_tmp")
git update-index --add --cacheinfo 100644,"$adopted_blob",ADOPTED_QUERY_ENGINE_IDEAS.md

git show HEAD:Makefile > "$makefile_tmp"
printf '%s\n' \
	'.PHONY: test-mz007-frontier-backpressure' \
	'test-mz007-frontier-backpressure:' \
	'	bash ./scripts/test-mz007-frontier-backpressure.sh' \
	'.PHONY: race-mz007-frontier-backpressure' \
	'race-mz007-frontier-backpressure:' \
	'	bash ./scripts/race-mz007-frontier-backpressure.sh' \
	'.PHONY: vet-mz007-frontier-backpressure' \
	'vet-mz007-frontier-backpressure:' \
	'	bash ./scripts/vet-mz007-frontier-backpressure.sh' \
	'.PHONY: format-mz007-frontier-backpressure' \
	'format-mz007-frontier-backpressure:' \
	'	bash ./scripts/format-mz007-frontier-backpressure.sh' \
	'.PHONY: benchmark-mz007-frontier-backpressure' \
	'benchmark-mz007-frontier-backpressure:' \
	'	bash ./scripts/benchmark-mz007-frontier-backpressure.sh' \
	'.PHONY: verify-mz007-frontier-backpressure' \
	'verify-mz007-frontier-backpressure:' \
	'	bash ./scripts/verify-mz007-frontier-backpressure.sh' \
	'.PHONY: review-mz007-frontier-backpressure' \
	'review-mz007-frontier-backpressure:' \
	'	bash ./scripts/review-mz007-frontier-backpressure.sh' \
	'.PHONY: stage-mz007-frontier-backpressure' \
	'stage-mz007-frontier-backpressure:' \
	'	bash ./scripts/stage-mz007-frontier-backpressure.sh' \
	'.PHONY: commit-mz007-frontier-backpressure' \
	'commit-mz007-frontier-backpressure:' \
	'	bash ./scripts/commit-mz007-frontier-backpressure.sh' \
	'.PHONY: push-mz007-frontier-backpressure' \
	'push-mz007-frontier-backpressure:' \
	'	bash ./scripts/push-mz007-frontier-backpressure.sh' >> "$makefile_tmp"
makefile_blob=$(git hash-object -w "$makefile_tmp")
git update-index --add --cacheinfo 100644,"$makefile_blob",Makefile

git diff --cached --check --
git status --short
