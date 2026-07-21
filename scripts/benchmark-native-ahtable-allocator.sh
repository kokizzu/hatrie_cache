#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
artifact_dir=${BENCHMARK_ARTIFACT_DIR:-build/benchmarks}
keys=${NATIVE_AHTABLE_KEYS:-100000}
slots=${NATIVE_AHTABLE_SLOTS:-4096}
count=${COUNT:-7}
output="$artifact_dir/native-ahtable-allocator.txt"
binary=$(mktemp "${TMPDIR:-/tmp}/hatrie-ahtable-bench.XXXXXX")
trap 'rm -f "$binary"' EXIT HUP INT TERM

mkdir -p "$artifact_dir"
gcc -O3 -std=c99 -Wall -Wextra \
	-I"$root/luikore__hat-trie/src" \
	-o "$binary" \
	"$root/luikore__hat-trie/test/bench_ahtable_allocator.c" \
	"$root/luikore__hat-trie/src/ahtable.c" \
	"$root/luikore__hat-trie/src/misc.c" \
	"$root/luikore__hat-trie/src/murmurhash3.c"

: > "$output"
run=1
while [ "$run" -le "$count" ]; do
	printf 'run=%s ' "$run" | tee -a "$output"
	"$binary" "$keys" "$slots" | tee -a "$output"
	run=$((run + 1))
done
