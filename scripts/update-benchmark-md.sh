#!/usr/bin/env sh
set -eu

artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
benchmark_md=${BENCHMARK_MD_PATH:-BENCHMARK.md}
start_comparison='<!-- BEGIN GENERATED COMMAND BENCHMARK COMPARISON -->'
end_comparison='<!-- END GENERATED COMMAND BENCHMARK COMPARISON -->'
start_raw='<!-- BEGIN GENERATED COMMAND BENCHMARK RAW RESULTS -->'
end_raw='<!-- END GENERATED COMMAND BENCHMARK RAW RESULTS -->'
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-benchmark-md.XXXXXX")

cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

fail() {
	echo "update-benchmark-md: $*" >&2
	exit 1
}

require_file() {
	path=$1
	[ -f "$path" ] || fail "missing $path"
}

first_line() {
	sed -n '1p' "$1"
}

append_memory_rows() {
	product=$1
	run=$2
	path=$3
	awk -F '\t' -v product="$product" -v run="$run" '
		NR > 1 {
			metric = $1
			value = $2
			if ($3 != "") {
				value = value " " $3
			}
			gsub(/\|/, "\\|", run)
			printf "| %s | `%s` | %s | %s |\n", product, run, metric, value
		}
	' "$path"
}

append_raw_section() {
	title=$1
	path=$2
	printf '### %s\n\n' "$title"
	printf '```text\n'
	cat "$path"
	printf '\n```\n\n'
}

replace_block() {
	start_marker=$1
	end_marker=$2
	insert_file=$3
	target=$4
	out_file="$tmp_dir/$(basename "$target").updated"

	grep -Fxq "$start_marker" "$target" || fail "missing start marker: $start_marker"
	grep -Fxq "$end_marker" "$target" || fail "missing end marker: $end_marker"

	awk -v start="$start_marker" -v end="$end_marker" -v insert="$insert_file" '
		BEGIN {
			while ((getline line < insert) > 0) {
				block = block line "\n"
			}
			close(insert)
		}
		$0 == start {
			print
			printf "%s", block
			in_block = 1
			seen_start = 1
			next
		}
		$0 == end {
			in_block = 0
			seen_end = 1
			print
			next
		}
		!in_block {
			print
		}
		END {
			if (!seen_start || !seen_end || in_block) {
				exit 1
			}
		}
	' "$target" >"$out_file" || fail "could not replace generated block in $target"
	mv "$out_file" "$target"
}

require_file "$benchmark_md"
require_file "$artifact_dir/hatrie-command-features.tsv"
require_file "$artifact_dir/hatrie-command-memory.tsv"
require_file "$artifact_dir/hatrie-command-features.md"
require_file "$artifact_dir/redis-command-features.tsv"
require_file "$artifact_dir/redis-command-memory.tsv"
require_file "$artifact_dir/redis-command-features.md"
require_file "$artifact_dir/tarantool-command-features.tsv"
require_file "$artifact_dir/tarantool-command-memory.tsv"
require_file "$artifact_dir/tarantool-command-features.md"

comparison_md="$tmp_dir/command-feature-comparison.md"
BENCHMARK_ARTIFACT_DIR="$artifact_dir" \
BENCHMARK_COMPARISON_MD="$comparison_md" \
	scripts/benchmark-command-comparison.sh >/dev/null

comparison_block="$tmp_dir/comparison-block.md"
{
	printf '## Memory Summary\n\n'
	printf 'Generated from command benchmark artifacts in `%s`.\n\n' "$artifact_dir"
	printf '| System | Run | Memory metric | Value |\n'
	printf '| --- | --- | --- | ---: |\n'
	append_memory_rows "HAT-trie cache" "$(first_line "$artifact_dir/hatrie-command-features.md")" "$artifact_dir/hatrie-command-memory.tsv"
	append_memory_rows "Redis" "$(first_line "$artifact_dir/redis-command-features.md")" "$artifact_dir/redis-command-memory.tsv"
	append_memory_rows "Tarantool" "$(first_line "$artifact_dir/tarantool-command-features.md")" "$artifact_dir/tarantool-command-memory.tsv"
	printf '\n'
	printf 'HAT-trie memory is the benchmark test process RSS, so it includes the Go runtime and test harness. Redis memory is server-reported memory from `INFO memory`. Tarantool memory is `/proc/self/status` RSS plus `box.slab.info()` values.\n\n'
	awk 'found || /^## HAT-trie vs Tarantool/ { found = 1; print }' "$comparison_md"
	printf '\n'
} >"$comparison_block"

raw_block="$tmp_dir/raw-block.md"
{
	printf '## Raw Results\n\n'
	printf 'Generated from raw command benchmark artifacts in `%s`.\n\n' "$artifact_dir"
	append_raw_section "Raw HAT-trie Command Result" "$artifact_dir/hatrie-command-features.md"
	append_raw_section "Raw Tarantool Result" "$artifact_dir/tarantool-command-features.md"
	append_raw_section "Raw Redis Result" "$artifact_dir/redis-command-features.md"
} >"$raw_block"

replace_block "$start_comparison" "$end_comparison" "$comparison_block" "$benchmark_md"
replace_block "$start_raw" "$end_raw" "$raw_block" "$benchmark_md"

echo "Updated $benchmark_md from $artifact_dir"
