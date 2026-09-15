#!/usr/bin/env bash
set -euo pipefail

source_root=.
source_tmp=""
output_tmp="$(mktemp -d)"
binary="$output_tmp/hatSql.test"
cleanup() {
	if [[ -n "$source_tmp" ]]; then
		rm -rf "$source_tmp"
	fi
	rm -rf "$output_tmp"
}
trap cleanup EXIT

if [[ ! -f hat/hatSql/asof_join.go ]]; then
	source_tmp="$(mktemp -d)"
	cp -a go.mod go.sum hat "$source_tmp/"
	git show HEAD:hat/hatSql/asof_join.go > "$source_tmp/hat/hatSql/asof_join.go"
	source_root="$source_tmp"
fi

(
	cd "$source_root"
	go test ./hat/hatSql -run '^$' -c -o "$binary"
)

for benchmark in Materialized Streaming; do
	printf '\n--- CH-U05 %s one-shot RSS ---\n' "$benchmark"
	/usr/bin/time -v "$binary" \
		-test.run '^$' \
		-test.bench "^BenchmarkCHU05ExternalWindow${benchmark}$" \
		-test.benchmem \
		-test.benchtime=1x \
		-test.count=1
done
