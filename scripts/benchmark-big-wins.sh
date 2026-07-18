#!/usr/bin/env sh
set -eu

bench=${BIG_WINS_BENCH:-^BenchmarkBigWins$}
benchtime=${BENCHTIME:-1x}
count=${COUNT:-1}
keys=${BIG_WINS_KEYS:-100000}
ops=${BIG_WINS_OPS:-100000}
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-big-wins-bench.XXXXXX")
binary="$tmp_dir/hatrie_cache.test"
output_file="$tmp_dir/output.txt"
time_file="$tmp_dir/time.txt"

cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

go test -c -o "$binary" .

printf 'HAT-trie big-wins benchmark: bench=%s benchtime=%s count=%s keys=%s ops=%s\n\n' "$bench" "$benchtime" "$count" "$keys" "$ops"

run_benchmark() {
	HATRIE_BIG_WINS_KEYS="$keys" HATRIE_BIG_WINS_OPS="$ops" \
		"$binary" -test.run '^$' -test.bench "$bench" -test.benchmem -test.count "$count" -test.benchtime "$benchtime"
}

if [ -x /usr/bin/time ]; then
	/usr/bin/time -v sh -c '
		HATRIE_BIG_WINS_KEYS="$1" HATRIE_BIG_WINS_OPS="$2" \
			"$0" -test.run "^$" -test.bench "$3" -test.benchmem -test.count "$4" -test.benchtime "$5"
	' "$binary" "$keys" "$ops" "$bench" "$count" "$benchtime" >"$output_file" 2>"$time_file"
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
