#!/usr/bin/env bash
set -euo pipefail

root=$(pwd)
tmp=$(mktemp -d /tmp/hatrie-c243-benchmark.XXXXXX)
trap 'rm -rf -- "$tmp"' EXIT

cp hat/hatStorage/remote_part_cache_c243_benchmark_test.go "$tmp/remote_part_cache_c243_benchmark_test.go"
git show HEAD:hat/hatStorage/remote_part.go >"$tmp/remote_part.go"
git show HEAD:hat/hatStorage/remote_part_cache.go >"$tmp/remote_part_cache.go"

printf '%s\n' '=== C243 baseline (HEAD) ==='
go test "$tmp/remote_part.go" "$tmp/remote_part_cache.go" "$tmp/remote_part_cache_c243_benchmark_test.go" \
	-run '^$' -bench '^BenchmarkC243RemotePartCache' -benchmem -count=5

printf '%s\n' '=== C243 candidate (working tree) ==='
go test "$root/hat/hatStorage/remote_part.go" "$root/hat/hatStorage/remote_part_cache.go" "$root/hat/hatStorage/remote_part_cache_c243_benchmark_test.go" \
	-run '^$' -bench '^BenchmarkC243RemotePartCache' -benchmem -count=5
