#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-5}
benchmark='^BenchmarkPersistentStorageBackend10k$'

mkdir -p "$artifact_dir"
test_binary="${TMPDIR:-/tmp}/hatrie-cache-storage-bench.$$"
trap 'rm -f "$test_binary"' EXIT HUP INT TERM
go test -c -o "$test_binary" .

for backend in LevelDB Pebble; do
	output="$artifact_dir/storage-${backend}.txt"
	timing="$artifact_dir/storage-${backend}.time.txt"
	/usr/bin/time -v -o "$timing" "$test_binary" \
		-test.run '^$' \
		-test.bench "${benchmark}/${backend}$" \
		-test.benchmem \
		-test.benchtime "$benchtime" \
		-test.count "$count" >"$output"
	cat "$output"
	awk '/Maximum resident set size/ { print "peak_rss_kib=" $NF }' "$timing"
done
