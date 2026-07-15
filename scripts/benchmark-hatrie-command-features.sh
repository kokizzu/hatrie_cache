#!/usr/bin/env sh
set -eu

bench=${HATRIE_BENCH:-^BenchmarkCommandFeature$}
benchtime=${BENCHTIME:-}
count=${COUNT:-1}
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-command-bench.XXXXXX")
binary="$tmp_dir/hatrie_cache.test"
output_file="$tmp_dir/output.txt"
time_file="$tmp_dir/time.txt"

cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

go test -c -o "$binary" .

printf 'HAT-trie benchmark: bench=%s benchtime=%s count=%s\n\n' "$bench" "${benchtime:-default}" "$count"

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
	max_rss_kib=$(awk -F: '/Maximum resident set size/ { gsub(/^[ \t]+/, "", $2); print $2 }' "$time_file")
	elapsed=$(sed -n 's/^.*Elapsed (wall clock) time.*: //p' "$time_file")
	printf '\nMemory summary:\n\n'
	printf '| Metric | Value |\n'
	printf '| --- | ---: |\n'
	printf '| Max resident set size | %s KiB |\n' "${max_rss_kib:-unknown}"
	printf '| Benchmark process wall time | %s |\n' "${elapsed:-unknown}"
else
	run_benchmark
	printf '\nMemory summary:\n\n'
	printf '| Metric | Value |\n'
	printf '| --- | ---: |\n'
	printf '| Max resident set size | not measured: /usr/bin/time unavailable |\n'
fi
