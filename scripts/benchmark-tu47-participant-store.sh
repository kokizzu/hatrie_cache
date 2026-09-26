#!/bin/sh
set -eu

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tu47-benchmark-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
output_file="$cache_dir/result.txt"
if GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU047ParticipantFileStore$' -benchmem -count=5 -v >"$output_file" 2>&1; then
	status=0
else
	status=$?
fi
if [ "$status" -eq 0 ] && ! rg -q '^BenchmarkTU047ParticipantFileStore/(marshal|save|load)-' "$output_file"; then
	printf '%s\n' 'T047 participant file-store benchmark produced no matching rows' >&2
	status=1
fi
cat "$output_file"
exit "$status"
