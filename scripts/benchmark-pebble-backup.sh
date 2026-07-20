#!/bin/sh
set -eu

bench=${PEBBLE_BACKUP_BENCH:-^BenchmarkPebbleCheckpointBackup10k$}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-5}
keys=${BACKUP_BENCH_KEYS:-10000}
artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
output="$artifact_dir/pebble-checkpoint-backup.txt"

mkdir -p "$artifact_dir"

echo "Pebble backup benchmark: bench=$bench benchtime=$benchtime count=$count keys=$keys"
echo

run_benchmark() {
	HATRIE_BACKUP_BENCH_KEYS="$keys" go test . \
		-run '^$' \
		-bench "$bench" \
		-benchmem \
		-benchtime "$benchtime" \
		-count "$count"
}

if command -v /usr/bin/time >/dev/null 2>&1; then
	time_output=$(mktemp)
	benchmark_output=$(mktemp)
	trap 'rm -f "$time_output" "$benchmark_output"' EXIT HUP INT TERM
	/usr/bin/time -f '%M %e' -o "$time_output" sh -c '
		HATRIE_BACKUP_BENCH_KEYS="$1" go test . -run "^$" -bench "$2" -benchmem -benchtime "$3" -count "$4"
	' sh "$keys" "$bench" "$benchtime" "$count" >"$benchmark_output"
	tee "$output" <"$benchmark_output"
	set -- $(cat "$time_output")
	{
		echo
		echo "Memory summary:"
		echo
		echo "| Metric | Value |"
		echo "| --- | ---: |"
		echo "| Max resident set size | $1 KiB |"
		echo "| Benchmark process wall time | $2 s |"
	} | tee -a "$output"
else
	run_benchmark | tee "$output"
fi

echo
echo "Raw result: $output"
