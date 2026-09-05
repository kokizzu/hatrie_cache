#!/bin/bash
set -euo pipefail

mode="${1:-check}"
case "$mode" in
check|commit|push)
	;;
*)
	printf 'usage: %s {check|commit|push}\n' "$0" >&2
	exit 2
	;;
esac

temp_dir=$(mktemp -d)
index_file="$temp_dir/index"
trap 'rm -rf "$temp_dir"' EXIT

GIT_INDEX_FILE="$index_file" git read-tree HEAD

append_benchmark() {
	local path="$1"
	if rg -q '^## Typed Aggregate Arrangement Hash Keys$' "$path"; then
		return
	fi
	printf '%s\n' \
		'' \
		'## Typed Aggregate Arrangement Hash Keys' \
		'' \
		'Five-run local benchmark on an AMD Ryzen 9 5950X, Go `amd64`, using the' \
		'existing typed aggregate arrangement workload. The optimized arrangement' \
		'hashes typed group values without allocating a formatted key for every' \
		'mutation. Exact collision buckets preserve correctness, and one legacy key' \
		'per live group preserves deterministic `Rows()` ordering.' \
		'' \
		'| Workload | Before | After | Improvement |' \
		'| --- | ---: | ---: | ---: |' \
		'| Independent two consumers | 2.817 ms; 1,470,917 B; 60,248 allocs | 1.718 ms; 41,192 B; 443 allocs | 1.64x faster; 35.71x lower heap; 136.00x fewer allocations |' \
		'| Shared two consumers | 1.434 ms; 742,242 B; 30,217 allocs | 0.894 ms; 30,328 B; 316 allocs | 1.60x faster; 24.47x lower heap; 95.62x fewer allocations |' \
		'' \
		'Raw output from `make benchmark-sql-typed-arrangements`:' \
		'' \
		'```text' \
		'Before independent: 2823731 1470917 B/op 60248 allocs/op; 2821445 1470930 60248; 2785670 1470916 60248; 2787432 1470917 60248; 2817434 1470917 60248' \
		'Before shared: 1406455 742242 B/op 30217 allocs/op; 1410190 742242 30217; 1433947 742241 30217; 1457972 742242 30217; 1463700 742242 30217' \
		'After independent: 1717944 41200 B/op 443 allocs/op; 1725849 41192 443; 1704107 41192 443; 1648318 41192 443; 1719211 41192 443' \
		'After shared: 889249 30328 B/op 316 allocs/op; 894965 30328 316; 892509 30328 316; 893720 30328 316; 900877 30328 316' \
		'```' >> "$path"
}

append_adopted() {
	local path="$1"
	if rg -q 'Typed compact keys for grouped arrangement state' "$path"; then
		return
	fi
	printf '%s\n' \
		'' \
		'| ClickHouse | Typed compact keys for grouped arrangement state | Implemented | `TypedTableAggregate` hashes typed group values without allocating a formatted key on every mutation, uses exact collision buckets, and retains one legacy key per live group for deterministic row ordering. See [BENCHMARK.md](BENCHMARK.md#typed-aggregate-arrangement-hash-keys). |' >> "$path"
}

append_makefile() {
	local path="$1"
	if rg -q '^test-typed-table-aggregate-key:' "$path"; then
		return
	fi
	printf '%s\n' \
		'' \
		'.PHONY: test-typed-table-aggregate-key' \
		'test-typed-table-aggregate-key:' \
		'	bash scripts/test-typed-table-aggregate-key.sh' \
		'' \
		'.PHONY: format-typed-table-aggregate-key' \
		'format-typed-table-aggregate-key:' \
		'	bash scripts/format-typed-table-aggregate-key.sh' \
		'' \
		'.PHONY: verify-typed-table-aggregate-key' \
		'verify-typed-table-aggregate-key:' \
		'	bash scripts/verify-typed-table-aggregate-key.sh' \
		'' \
		'.PHONY: test-race-typed-table-aggregate-key' \
		'test-race-typed-table-aggregate-key:' \
		'	bash scripts/test-race-typed-table-aggregate-key.sh' \
		'' \
		'.PHONY: commit-typed-table-aggregate-key' \
		'commit-typed-table-aggregate-key:' \
		'	bash scripts/deliver-typed-table-aggregate-key.sh commit' \
		'' \
		'.PHONY: push-typed-table-aggregate-key' \
		'push-typed-table-aggregate-key:' \
		'	bash scripts/deliver-typed-table-aggregate-key.sh push' >> "$path"
}

git show HEAD:BENCHMARK.md > "$temp_dir/BENCHMARK.md"
git show HEAD:ADOPTED_QUERY_ENGINE_IDEAS.md > "$temp_dir/ADOPTED_QUERY_ENGINE_IDEAS.md"
git show HEAD:Makefile > "$temp_dir/Makefile"

append_benchmark "$temp_dir/BENCHMARK.md"
append_adopted "$temp_dir/ADOPTED_QUERY_ENGINE_IDEAS.md"
append_makefile "$temp_dir/Makefile"

stage_generated() {
	local source="$1"
	local path="$2"
	local object
	object=$(git hash-object -w "$source")
	GIT_INDEX_FILE="$index_file" git update-index --add --cacheinfo "100644,$object,$path"
}

stage_generated "$temp_dir/BENCHMARK.md" BENCHMARK.md
stage_generated "$temp_dir/ADOPTED_QUERY_ENGINE_IDEAS.md" ADOPTED_QUERY_ENGINE_IDEAS.md
stage_generated "$temp_dir/Makefile" Makefile

GIT_INDEX_FILE="$index_file" git add -- \
	hat/hatSql/typed_table.go \
	hat/hatSql/typed_table_aggregate_key.go \
	hat/hatSql/typed_table_aggregate_key_test.go \
	scripts/test-typed-table-aggregate-key.sh \
	scripts/format-typed-table-aggregate-key.sh \
	scripts/verify-typed-table-aggregate-key.sh \
	scripts/test-race-typed-table-aggregate-key.sh \
	scripts/deliver-typed-table-aggregate-key.sh

GIT_INDEX_FILE="$index_file" git diff --cached --check
printf '%s\n' '== isolated feature paths =='
GIT_INDEX_FILE="$index_file" git diff --cached --name-only
printf '%s\n' '== isolated feature stat =='
GIT_INDEX_FILE="$index_file" git diff --cached --stat

case "$mode" in
check)
	;;
commit)
	GIT_INDEX_FILE="$index_file" git commit -m 'perf(sql): use typed hash keys for aggregates'
	;;
push)
	branch=$(git branch --show-current)
	git push origin "$branch"
	;;
esac
