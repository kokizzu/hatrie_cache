#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
keys=${NATIVE_HATTRIE_KEYS:-100000}
lookup_operations=${NATIVE_HATTRIE_LOOKUPS:-10000000}
key_mode=${NATIVE_HATTRIE_KEY_MODE:-shared}
insert_repetitions=${NATIVE_HATTRIE_INSERT_REPETITIONS:-1}
count=${COUNT:-7}
output="$artifact_dir/native-hattrie-lookup.txt"
binary=$(mktemp "${TMPDIR:-/tmp}/hatrie-lookup-bench.XXXXXX")
trap 'rm -f "$binary"' EXIT HUP INT TERM

case "$key_mode" in
	shared|distributed)
		;;
	*)
		echo "NATIVE_HATTRIE_KEY_MODE must be shared or distributed" >&2
		exit 2
		;;
esac

mkdir -p "$artifact_dir"
gcc -O3 -std=c99 -Wall -Wextra \
	-I"$root/luikore__hat-trie/src" \
	-o "$binary" \
	"$root/luikore__hat-trie/test/bench_hattrie_lookup.c" \
	"$root/luikore__hat-trie/src/hat-trie.c" \
	"$root/luikore__hat-trie/src/ahtable.c" \
	"$root/luikore__hat-trie/src/misc.c" \
	"$root/luikore__hat-trie/src/murmurhash3.c"

: > "$output"
run=1
while [ "$run" -le "$count" ]; do
	printf 'run=%s ' "$run" | tee -a "$output"
	"$binary" "$keys" "$lookup_operations" "$key_mode" "$insert_repetitions" | tee -a "$output"
	run=$((run + 1))
done
