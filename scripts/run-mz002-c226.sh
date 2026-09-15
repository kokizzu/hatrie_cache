#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
tmp_dir="$(mktemp -d /tmp/hatrie-cache-mz002-c226.XXXXXX)"
cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT

git archive --format=tar HEAD | tar -x -C "$tmp_dir"
paths=(hat/hatSql/mz002_typed_table_compact_benchmark_test.go)
if [[ "$mode" != "baseline" ]]; then
	paths+=(
		hat/hatSql/mz002_typed_table_read_hold_test.go
		hat/hatSql/mz002_typed_table_read_hold_benchmark_test.go
		hat/hatSql/typed_table.go
		hat/hatSql/typed_table_change_read_hold.go
	)
fi
for path in "${paths[@]}"; do
	if [[ -f "$path" ]]; then
		mkdir -p "$tmp_dir/$(dirname "$path")"
		cp "$path" "$tmp_dir/$path"
	fi
done

case "$mode" in
test)
	go_args=(test ./hat/hatSql -run 'TestMZ002' -count=1)
	;;
all)
	go_args=(test ./hat/hatSql -count=1)
	;;
race)
	go_args=(test -race ./hat/hatSql -run 'TestMZ002' -count=1)
	;;
benchmark)
	go_args=(test ./hat/hatSql -run '^$' -bench 'BenchmarkMZ002TypedTable(CompactNoHold|ReadHoldLifecycle)$' -benchmem -count="${MZ002_BENCH_COUNT:-5}")
	;;
baseline)
	go_args=(test ./hat/hatSql -run '^$' -bench 'BenchmarkMZ002TypedTableCompactNoHold' -benchmem -count="${MZ002_BENCH_COUNT:-5}")
	;;
vet)
	go_args=(vet ./hat/hatSql)
	;;
*)
printf 'usage: %s {test|all|race|benchmark|baseline|vet}\n' "$0" >&2
	exit 2
	;;
esac

(cd "$tmp_dir" && go "${go_args[@]}")
