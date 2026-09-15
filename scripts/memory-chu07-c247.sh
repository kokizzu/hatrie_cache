#!/usr/bin/env bash
set -euo pipefail

source_root=.
temporary_root=""
binary_path=""
cleanup() {
	if [[ -n "$temporary_root" ]]; then
		rm -rf "$temporary_root"
	fi
	if [[ -n "$binary_path" ]]; then
		rm -f "$binary_path"
	fi
}
trap cleanup EXIT

if [[ ! -f hat/hatSql/asof_join.go ]]; then
	temporary_root="$(mktemp -d)"
	cp -a go.mod go.sum hat internal "$temporary_root/"
	git show HEAD:hat/hatSql/asof_join.go > "$temporary_root/hat/hatSql/asof_join.go"
	source_root="$temporary_root"
fi

binary_path="$(mktemp)"
(cd "$source_root" && go test -c -o "$binary_path" ./hat/hatCache)
/usr/bin/time -v "$binary_path" -test.run '^$' -test.bench '^BenchmarkCHU07MutationStatusLookup$' -test.benchmem -test.benchtime=1x
