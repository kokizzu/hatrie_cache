#!/usr/bin/env bash
set -euo pipefail

paths=(
	README.md
	ENGINE_IDEAS.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	SQL_PLANNER_STATISTICS.md
	hat/hatCache/sql_planner_statistics_persistence.go
	hat/hatCache/sql_planner_statistics_test.go
	hat/hatCache/tt050_sql_planner_statistics_persistence_benchmark_test.go
	scripts/benchmark-before-tt050-durable-stats-c292.sh
	scripts/benchmark-tt050-durable-stats-c292.sh
	scripts/format-tt050-durable-stats-c292.sh
	scripts/test-tt050-durable-stats-c292.sh
	scripts/verify-tt050-durable-stats-c292.sh
	scripts/review-tt050-durable-stats-c292.sh
	scripts/stage-tt050-durable-stats-c292.sh
	scripts/inspect-staged-tt050-durable-stats-c292.sh
	scripts/commit-tt050-durable-stats-c292.sh
	scripts/push-tt050-durable-stats-c292.sh
)

if ! git diff --cached --quiet; then
	echo "refusing stage with pre-existing staged changes" >&2
	exit 1
fi

for path in "${paths[@]}"; do
	if [[ ! -e "$path" ]]; then
		echo "missing expected TT-050 path: $path" >&2
		exit 1
	fi
done

git add -- "${paths[@]}"

workdir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt050-stage.XXXXXX")
trap 'rm -rf "$workdir"' EXIT
base="$workdir/Makefile.base"
candidate="$workdir/Makefile.candidate"
raw_patch="$workdir/Makefile.patch"
normalized_patch="$workdir/Makefile.normalized.patch"

git show HEAD:Makefile > "$base"
cp "$base" "$candidate"
printf '%s\n' \
'' \
'.PHONY: test-tt050-durable-stats-c292' \
'test-tt050-durable-stats-c292:' \
$'\t@bash scripts/test-tt050-durable-stats-c292.sh' \
'' \
'.PHONY: format-tt050-durable-stats-c292' \
'format-tt050-durable-stats-c292:' \
$'\t@bash scripts/format-tt050-durable-stats-c292.sh' \
'' \
'.PHONY: benchmark-before-tt050-durable-stats-c292' \
'benchmark-before-tt050-durable-stats-c292:' \
$'\t@bash scripts/benchmark-before-tt050-durable-stats-c292.sh' \
'' \
'.PHONY: benchmark-tt050-durable-stats-c292' \
'benchmark-tt050-durable-stats-c292:' \
$'\t@bash scripts/benchmark-tt050-durable-stats-c292.sh' \
'' \
'.PHONY: verify-tt050-durable-stats-c292' \
'verify-tt050-durable-stats-c292:' \
$'\t@bash scripts/verify-tt050-durable-stats-c292.sh' \
'' \
'.PHONY: review-tt050-durable-stats-c292' \
'review-tt050-durable-stats-c292:' \
$'\t@bash scripts/review-tt050-durable-stats-c292.sh' \
'' \
'.PHONY: stage-tt050-durable-stats-c292' \
'stage-tt050-durable-stats-c292:' \
$'\t@bash scripts/stage-tt050-durable-stats-c292.sh' \
'' \
'.PHONY: inspect-staged-tt050-durable-stats-c292' \
'inspect-staged-tt050-durable-stats-c292:' \
$'\t@bash scripts/inspect-staged-tt050-durable-stats-c292.sh' \
'' \
'.PHONY: commit-tt050-durable-stats-c292' \
'commit-tt050-durable-stats-c292:' \
$'\t@bash scripts/commit-tt050-durable-stats-c292.sh' \
'' \
'.PHONY: push-tt050-durable-stats-c292' \
'push-tt050-durable-stats-c292:' \
$'\t@bash scripts/push-tt050-durable-stats-c292.sh' >> "$candidate"

diff_status=0
git diff --no-index --no-ext-diff -- "$base" "$candidate" > "$raw_patch" || diff_status=$?
if [[ "$diff_status" -ne 1 ]]; then
	echo "unexpected Makefile diff status: $diff_status" >&2
	exit 1
fi
sed -e "s|$base|a/Makefile|g" -e "s|$candidate|b/Makefile|g" "$raw_patch" > "$normalized_patch"
git apply --cached -- "$normalized_patch"
git diff --cached --check
