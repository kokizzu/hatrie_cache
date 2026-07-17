#!/usr/bin/env sh
set -eu

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-benchmark-md-test.XXXXXX")
artifact_dir="$tmp_dir/artifacts"
doc="$tmp_dir/BENCHMARK.md"

cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

assert_contains() {
	pattern=$1
	path=$2
	grep -Fq "$pattern" "$path" || {
		echo "verify-benchmark-md-update: missing pattern: $pattern" >&2
		exit 1
	}
}

assert_not_contains() {
	pattern=$1
	path=$2
	if grep -Fq "$pattern" "$path"; then
		echo "verify-benchmark-md-update: unexpected pattern: $pattern" >&2
		exit 1
	fi
}

mkdir -p "$artifact_dir"

printf 'benchmark\titerations\tns_per_op\tbytes_per_op\tallocs_per_op\tseconds_per_10k\n' >"$artifact_dir/hatrie-command-features.tsv"
printf 'BenchmarkCommandFeature/StringSet\t10\t100\t8\t1\t0.001000\n' >>"$artifact_dir/hatrie-command-features.tsv"
printf 'BenchmarkCommandFeature/StringGet\t10\t50\t0\t0\t0.000500\n' >>"$artifact_dir/hatrie-command-features.tsv"
printf 'metric\tvalue\tunit\n' >"$artifact_dir/hatrie-command-memory.tsv"
printf 'Max resident set size\t12345\tKiB\n' >>"$artifact_dir/hatrie-command-memory.tsv"
printf 'HAT-trie benchmark: bench=fixture benchtime=10x count=1\n\nBenchmarkCommandFeature/StringSet-32 10 100 ns/op 8 B/op 1 allocs/op\n' >"$artifact_dir/hatrie-command-features.md"

printf 'feature\tcommand\tthroughput_req_per_sec\tseconds_per_10k\n' >"$artifact_dir/redis-command-features.tsv"
printf 'String write\tSET key value\t10000\t1.000000\n' >>"$artifact_dir/redis-command-features.tsv"
printf 'String read\tGET key\t20000\t0.500000\n' >>"$artifact_dir/redis-command-features.tsv"
printf 'metric\tvalue\tunit\n' >"$artifact_dir/redis-command-memory.tsv"
printf 'used_memory\t999\tB\n' >>"$artifact_dir/redis-command-memory.tsv"
printf 'Redis benchmark: host=127.0.0.1 port=6380 requests=10 clients=1 keyspace=10\n\n| Feature family | Redis command | Throughput | Seconds / 10k ops |\n| --- | --- | ---: | ---: |\n| String write | `SET key value` | 10000.00 req/s | 1.000000 s |\n' >"$artifact_dir/redis-command-features.md"

printf 'feature\toperation\tseconds_per_10k\n' >"$artifact_dir/tarantool-command-features.tsv"
printf 'String write\tspace:replace()\t0.010000\n' >>"$artifact_dir/tarantool-command-features.tsv"
printf 'String read\tspace.index.primary:get()\t0.005000\n' >>"$artifact_dir/tarantool-command-features.tsv"
printf 'metric\tvalue\tunit\n' >"$artifact_dir/tarantool-command-memory.tsv"
printf 'Process RSS\t555\tKiB\n' >>"$artifact_dir/tarantool-command-memory.tsv"
printf 'Tarantool benchmark: version=fixture requests=10 keyspace=10\n\n| Feature family | Tarantool operation | Seconds / 10k feature cycles |\n| --- | --- | ---: |\n| String write | `space:replace()` | 0.010000 s |\n' >"$artifact_dir/tarantool-command-features.md"

{
	printf '# Benchmark\n\n'
	printf 'Before text.\n\n'
	printf '<!-- BEGIN GENERATED COMMAND BENCHMARK COMPARISON -->\n'
	printf 'old comparison\n'
	printf '<!-- END GENERATED COMMAND BENCHMARK COMPARISON -->\n\n'
	printf 'Middle text.\n\n'
	printf '<!-- BEGIN GENERATED COMMAND BENCHMARK RAW RESULTS -->\n'
	printf 'old raw\n'
	printf '<!-- END GENERATED COMMAND BENCHMARK RAW RESULTS -->\n\n'
	printf 'After text.\n'
} >"$doc"

BENCHMARK_ARTIFACT_DIR="$artifact_dir" BENCHMARK_MD_PATH="$doc" scripts/update-benchmark-md.sh >/dev/null

assert_contains 'Before text.' "$doc"
assert_contains 'Middle text.' "$doc"
assert_contains 'After text.' "$doc"
assert_contains 'Generated from command benchmark artifacts' "$doc"
assert_contains '| HAT-trie cache | `HAT-trie benchmark: bench=fixture benchtime=10x count=1` | Max resident set size | 12345 KiB |' "$doc"
assert_contains '| String write | `BenchmarkCommandFeature/StringSet` | 0.001000 s | 8 B/op | `space:replace()` | 0.010000 s | 10.00x |' "$doc"
assert_contains '| String read | `BenchmarkCommandFeature/StringGet` | 0.000500 s | `GET key` | 0.500000 s | 1000.00x |' "$doc"
assert_contains '### Raw Redis Result' "$doc"
assert_contains 'Redis benchmark: host=127.0.0.1 port=6380 requests=10 clients=1 keyspace=10' "$doc"
assert_not_contains 'old comparison' "$doc"
assert_not_contains 'old raw' "$doc"

echo "verify-benchmark-md-update: ok"
