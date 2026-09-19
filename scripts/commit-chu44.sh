#!/usr/bin/env bash
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
head=$(git rev-parse HEAD)
stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu44-commit.XXXXXX")
index="$stage/index"
trap 'rm -rf "$stage"' EXIT

git archive "$head" | tar -x -C "$stage"

awk '
{
	if (index($0, "return normalizeSQLAggregateStateIf(expr)") != 0) {
		done = 1
	}
	if (!done && index($0, "return expr, false, nil") != 0) {
		print "\t\treturn normalizeSQLAggregateStateIf(expr)"
		done = 1
		next
	}
	print
}
END {
	if (!done) {
		exit 1
	}
}' "$stage/hat/hatSql/aggregate_if.go" > "$stage/hat/hatSql/aggregate_if.go.tmp"
mv "$stage/hat/hatSql/aggregate_if.go.tmp" "$stage/hat/hatSql/aggregate_if.go"

if ! rg -q 'COUNT_STATE_IF' "$stage/hat/hatSql/query.go"; then
	sed -i 's/"ARGMAX_MERGE",/"ARGMAX_MERGE", "COUNT_STATE_IF", "SUM_STATE_IF", "AVG_STATE_IF", "MIN_STATE_IF", "MAX_STATE_IF", "COUNT_MERGE_IF", "SUM_MERGE_IF", "AVG_MERGE_IF", "MIN_MERGE_IF", "MAX_MERGE_IF", "ARGMAX_STATE_IF", "ARGMIN_STATE_IF", "ARGMAX_MERGE_IF", "ARGMIN_MERGE_IF",/' "$stage/hat/hatSql/query.go"
fi

if ! rg -q '^format-chu44:' "$stage/Makefile"; then
	printf '%s\n' \
		'' \
		'.PHONY: test-chu44 benchmark-chu44' \
		'test-chu44:' \
		$'\tbash ./scripts/test-chu44.sh' \
		'benchmark-chu44:' \
		$'\tbash ./scripts/benchmark-chu44.sh' \
		'' \
		'.PHONY: format-chu44' \
		'format-chu44:' \
		$'\tbash ./scripts/format-chu44.sh' \
		'' \
		'.PHONY: test-chu44-package' \
		'test-chu44-package:' \
		$'\tbash ./scripts/test-chu44-package.sh' \
		'' \
		'.PHONY: race-chu44' \
		'race-chu44:' \
		$'\tbash ./scripts/race-chu44.sh' \
		'' \
		'.PHONY: vet-chu44' \
		'vet-chu44:' \
		$'\tbash ./scripts/vet-chu44.sh' \
		'' \
		'.PHONY: verify-chu44' \
		'verify-chu44:' \
		$'\tbash ./scripts/verify-chu44.sh' \
		'' \
		'.PHONY: commit-chu44 push-chu44' \
		'commit-chu44:' \
		$'\tbash ./scripts/commit-chu44.sh' \
		'push-chu44:' \
		$'\tbash ./scripts/push-chu44.sh' \
		>> "$stage/Makefile"
fi

awk '
{
	if (index($0, "CHU44_SQL_AGGREGATE_COMBINATORS.md") != 0) {
		done = 1
	}
	print
	if (!done && index($0, "AGGREGATE_COMBINATORS.md)") != 0) {
		print "- Filtered SQL aggregate state/merge combinators such as `SUM_STATE_IF` and `SUM_MERGE_IF`: [CHU44_SQL_AGGREGATE_COMBINATORS.md](CHU44_SQL_AGGREGATE_COMBINATORS.md), with raw measurements in [BENCHMARK.md](BENCHMARK.md#ch-u44-sql-aggregate-combinators)"
		done = 1
	}
}
END {
	if (!done) {
		exit 1
	}
}' "$stage/README.md" > "$stage/README.md.tmp"
mv "$stage/README.md.tmp" "$stage/README.md"

awk -v replacement='| CH-U44 | SQL aggregate combinators | Implemented filtered built-in state and merge forms including `*_STATE_IF`, `*_MERGE_IF`, arg-extreme variants, strict arity diagnostics, and composition with `FILTER (WHERE ...)`. See [CHU44_SQL_AGGREGATE_COMBINATORS.md](CHU44_SQL_AGGREGATE_COMBINATORS.md) and [BENCHMARK.md#ch-u44-sql-aggregate-combinators](BENCHMARK.md#ch-u44-sql-aggregate-combinators). | Wider planner integration and SQL parser names for custom registered combinators. |' '
{
	if (index($0, "| CH-U44 |") == 1) {
		print replacement
		found = 1
		next
	}
	print
}
END {
	if (!found) {
		exit 1
	}
}' "$stage/PRODUCT_IDEA_GAPS.md" > "$stage/PRODUCT_IDEA_GAPS.md.tmp"
mv "$stage/PRODUCT_IDEA_GAPS.md.tmp" "$stage/PRODUCT_IDEA_GAPS.md"

if ! rg -q '^## CH-U44 SQL Aggregate Combinators$' "$stage/BENCHMARK.md"; then
printf '%s\n' \
  '' \
  '## CH-U44 SQL Aggregate Combinators' \
  '' \
  'The benchmark uses a deterministic 128-row `VALUES` source and executes the' \
  'query through the same parser and executor path. Values below are five raw' \
  '`-benchmem` samples on Linux/amd64 with an AMD Ryzen 9 5950X. The clean' \
  'baseline was captured before the feature; the two after rows were captured by' \
  '`make benchmark-chu44` after the implementation was optimized to share the' \
  'existing aggregate normalization pass.' \
  '' \
  '| Operation | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | CPU vs clean baseline |' \
  '| --- | --- | ---: | ---: | ---: | ---: |' \
  '| Before: existing `SUM_STATE(...) FILTER` | 86,366; 87,541; 89,298; 89,633; 94,293 | 89,298 | 115,768 | 608 | 1.00x |' \
  '| After: existing `SUM_STATE(...) FILTER` control | 86,327; 93,376; 86,419; 88,605; 91,161 | 88,605 | 115,768 | 608 | 0.99x |' \
  '| After: `SUM_STATE_IF(...)` | 87,924; 87,663; 89,543; 92,229; 92,607 | 89,543 | 115,768 | 608 | 1.00x (0.27% slower) |' \
  '' \
  'The new spelling has the same measured heap and allocation profile as the' \
  'existing filtered spelling. Its median CPU cost is within benchmark noise of' \
  'the clean baseline; the small 0.27% difference is not treated as a meaningful' \
  'regression. The implementation adds no retained state memory and leaves the' \
  'serialized state bytes unchanged. Details and examples are in' \
  '[CHU44_SQL_AGGREGATE_COMBINATORS.md](CHU44_SQL_AGGREGATE_COMBINATORS.md).' \
  >> "$stage/BENCHMARK.md"
fi

feature_paths=(
	CHU44_SQL_AGGREGATE_COMBINATORS.md
	hat/hatSql/aggregate_state_if.go
	hat/hatSql/chu44_aggregate_combinator_benchmark_test.go
	hat/hatSql/chu44_aggregate_combinator_test.go
	scripts/benchmark-chu44.sh
	scripts/commit-chu44.sh
	scripts/format-chu44.sh
	scripts/push-chu44.sh
	scripts/race-chu44.sh
	scripts/test-chu44-package.sh
	scripts/test-chu44.sh
	scripts/verify-chu44.sh
	scripts/vet-chu44.sh
)
for path in "${feature_paths[@]}"; do
	cp "$repo/$path" "$stage/$path"
done

GIT_INDEX_FILE="$index" git read-tree "$head"
GIT_INDEX_FILE="$index" git --work-tree="$stage" add -A -- \
	Makefile README.md BENCHMARK.md PRODUCT_IDEA_GAPS.md \
	hat/hatSql/aggregate_if.go hat/hatSql/query.go \
	"${feature_paths[@]}"

printf '%s\n' '== CH-U44 isolated staged diff =='
GIT_INDEX_FILE="$index" git diff --cached --stat

if [ "$(git rev-parse HEAD)" != "$head" ]; then
	echo 'HEAD changed while preparing CH-U44 commit; aborting' >&2
	exit 1
fi

tree=$(GIT_INDEX_FILE="$index" git write-tree)
commit=$(printf '%s\n' 'fix(hatSql): wire CH-U44 parser and Makefile targets' | git commit-tree "$tree" -p "$head")

if ref=$(git symbolic-ref -q HEAD); then
	git update-ref "$ref" "$commit" "$head"
else
	git update-ref HEAD "$commit" "$head"
fi
printf 'CH-U44 commit: %s\n' "$commit"
