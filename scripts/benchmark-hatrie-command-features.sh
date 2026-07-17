#!/usr/bin/env sh
set -eu

bench=${HATRIE_BENCH:-^BenchmarkCommandFeature$}
benchtime=${BENCHTIME:-}
count=${COUNT:-1}
artifact_dir=${BENCHMARK_ARTIFACT_DIR:-${HATRIE_BENCHMARK_ARTIFACT_DIR:-}}
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-command-bench.XXXXXX")
binary="$tmp_dir/hatrie_cache.test"
output_file="$tmp_dir/output.txt"
time_file="$tmp_dir/time.txt"
raw_file=
rows_tsv=
memory_tsv=

cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

if [ -n "$artifact_dir" ]; then
	mkdir -p "$artifact_dir"
	raw_file="$artifact_dir/hatrie-command-features.md"
	rows_tsv="$artifact_dir/hatrie-command-features.tsv"
	memory_tsv="$artifact_dir/hatrie-command-memory.tsv"
	: >"$raw_file"
	printf 'benchmark\titerations\tns_per_op\tbytes_per_op\tallocs_per_op\tseconds_per_10k\n' >"$rows_tsv"
	printf 'metric\tvalue\tunit\n' >"$memory_tsv"
fi

emit() {
	printf "$@"
	if [ -n "$raw_file" ]; then
		printf "$@" >>"$raw_file"
	fi
}

record_benchmark_rows() {
	if [ -z "$rows_tsv" ]; then
		return
	fi
	awk '
		/^Benchmark/ {
			benchmark = $1
			sub(/-[0-9]+$/, "", benchmark)
			iterations = $2
			ns_per_op = $3
			bytes_per_op = $5
			allocs_per_op = $7
			seconds_per_10k = ns_per_op * 10000 / 1000000000
			printf "%s\t%s\t%s\t%s\t%s\t%.6f\n", benchmark, iterations, ns_per_op, bytes_per_op, allocs_per_op, seconds_per_10k
		}
	' "$output_file" >>"$rows_tsv"
}

go test -c -o "$binary" .

emit 'HAT-trie benchmark: bench=%s benchtime=%s count=%s\n\n' "$bench" "${benchtime:-default}" "$count"

run_benchmark() {
	if [ -n "$benchtime" ]; then
		"$binary" -test.run '^$' -test.bench "$bench" -test.benchmem -test.count "$count" -test.benchtime "$benchtime"
	else
		"$binary" -test.run '^$' -test.bench "$bench" -test.benchmem -test.count "$count"
	fi
}

if [ -x /usr/bin/time ]; then
	/usr/bin/time -v sh -c '
		if [ -n "$1" ]; then
			"$0" -test.run "^$" -test.bench "$2" -test.benchmem -test.count "$3" -test.benchtime "$1"
		else
			"$0" -test.run "^$" -test.bench "$2" -test.benchmem -test.count "$3"
		fi
	' "$binary" "$benchtime" "$bench" "$count" >"$output_file" 2>"$time_file"
	cat "$output_file"
	if [ -n "$raw_file" ]; then
		cat "$output_file" >>"$raw_file"
	fi
	record_benchmark_rows
	max_rss_kib=$(awk -F: '/Maximum resident set size/ { gsub(/^[ \t]+/, "", $2); print $2 }' "$time_file")
	elapsed=$(sed -n 's/^.*Elapsed (wall clock) time.*: //p' "$time_file")
	emit '\nMemory summary:\n\n'
	emit '| Metric | Value |\n'
	emit '| --- | ---: |\n'
	emit '| Max resident set size | %s KiB |\n' "${max_rss_kib:-unknown}"
	emit '| Benchmark process wall time | %s |\n' "${elapsed:-unknown}"
	if [ -n "$memory_tsv" ]; then
		printf 'Max resident set size\t%s\tKiB\n' "${max_rss_kib:-unknown}" >>"$memory_tsv"
		printf 'Benchmark process wall time\t%s\t\n' "${elapsed:-unknown}" >>"$memory_tsv"
	fi
else
	run_benchmark >"$output_file"
	cat "$output_file"
	if [ -n "$raw_file" ]; then
		cat "$output_file" >>"$raw_file"
	fi
	record_benchmark_rows
	emit '\nMemory summary:\n\n'
	emit '| Metric | Value |\n'
	emit '| --- | ---: |\n'
	emit '| Max resident set size | not measured: /usr/bin/time unavailable |\n'
	if [ -n "$memory_tsv" ]; then
		printf 'Max resident set size\tnot measured: /usr/bin/time unavailable\t\n' >>"$memory_tsv"
	fi
fi
