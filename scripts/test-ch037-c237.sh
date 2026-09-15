#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
tmp_dir="$(mktemp -d /tmp/hatrie-cache-ch037.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
git show HEAD:hat/hatSql/asof_join.go > "$tmp_dir/hat/hatSql/asof_join.go"
cp hat/hatSql/ch037_arg_extreme_state_test.go "$tmp_dir/hat/hatSql/"
if [[ -f hat/hatSql/ch037_arg_extreme_state_codec_test.go ]]; then
    cp hat/hatSql/ch037_arg_extreme_state_codec_test.go "$tmp_dir/hat/hatSql/"
fi
if [[ -f hat/hatSql/ch037_arg_extreme_state_benchmark_test.go ]]; then
    cp hat/hatSql/ch037_arg_extreme_state_benchmark_test.go "$tmp_dir/hat/hatSql/"
fi
if [[ -f hat/hatSql/arg_extreme_state.go ]]; then
    cp hat/hatSql/arg_extreme_state.go "$tmp_dir/hat/hatSql/"
fi
if [[ -f hat/hatSql/arg_extreme.go ]]; then
    cp hat/hatSql/arg_extreme.go "$tmp_dir/hat/hatSql/"
fi
if [[ -f hat/hatSql/query.go ]]; then
    cp hat/hatSql/query.go "$tmp_dir/hat/hatSql/"
fi
cd "$tmp_dir"

case "$mode" in
test)
    go test ./hat/hatSql -run '^TestCH037' -count=1
    ;;
package)
    go test ./hat/hatSql -count=1
    ;;
vet)
    go vet ./hat/hatSql
    ;;
race)
    go test -race ./hat/hatSql -run '^TestCH037' -count=1
    ;;
benchmark)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH037' -benchmem -benchtime "${BENCHTIME:-200ms}" -count "${BENCHCOUNT:-5}"
    ;;
*)
    printf 'unsupported mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
