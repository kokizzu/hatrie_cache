#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
tmp_dir="$(mktemp -d /tmp/hatrie-cache-ch225.XXXXXX)"
cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT

git archive --format=tar HEAD | tar -x -C "$tmp_dir"
paths=(
	hat/hatSql/ch007_row_ttl_benchmark_test.go
)
if [[ "$mode" != "baseline" ]]; then
	paths+=(
		hat/hatSql/typed_table_ttl.go
		hat/hatSql/typed_table.go
		hat/hatSql/typed_table_patch_parts.go
		hat/hatSql/typed_table_ttl_expiry_index.go
	)
fi
if [[ "$mode" == "test" || "$mode" == "race" || "$mode" == "all" ]]; then
	paths+=(hat/hatSql/ch225_ttl_expiry_index_test.go)
fi
if [[ "$mode" == "benchmark" ]]; then
	paths+=(hat/hatSql/ch225_ttl_memory_benchmark_test.go)
fi
for path in "${paths[@]}"; do
	if [[ -f "$path" ]]; then
		mkdir -p "$tmp_dir/$(dirname "$path")"
		cp "$path" "$tmp_dir/$path"
	fi
done

case "$mode" in
test)
	go_args=(test ./hat/hatSql -run 'Test(CH007|CH225)' -count=1)
	;;
benchmark)
	go_args=(test ./hat/hatSql -run '^$' -bench 'Benchmark(CH007TypedTable(PurgeExpiredNoop|PurgeExpiredSparse|UpsertProcessingTTL|DeleteReinsertProcessingTTL)|CH225TypedTableExpiryIndexMemory)$' -benchmem -count="${CH225_BENCH_COUNT:-5}")
	;;
baseline)
	go_args=(test ./hat/hatSql -run '^$' -bench 'BenchmarkCH007TypedTable(PurgeExpiredNoop|PurgeExpiredSparse|UpsertProcessingTTL|DeleteReinsertProcessingTTL)$' -benchmem -count="${CH225_BENCH_COUNT:-5}")
	;;
race)
	go_args=(test -race ./hat/hatSql -run 'Test(CH007|CH225)' -count=1)
	;;
all)
	go_args=(test ./hat/hatSql -count=1)
	;;
vet)
	go_args=(vet ./hat/hatSql)
	;;
*)
printf 'usage: %s {test|all|benchmark|baseline|race|vet}\n' "$0" >&2
	exit 2
	;;
esac

(cd "$tmp_dir" && go "${go_args[@]}")
