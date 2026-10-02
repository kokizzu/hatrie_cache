#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
tmpdir=$(mktemp -d /tmp/hatrie-cache-t-u18-go-build.XXXXXX)
trap 'rm -rf "$tmpdir"' EXIT
export GOTMPDIR="$tmpdir"

focused_files=(
  ./hat/hatDataStructure/volatile_engine.go
  ./hat/hatDataStructure/volatile_engine_test.go
)
if [[ -f hat/hatDataStructure/volatile_engine_benchmark_test.go ]]; then
  focused_files+=(./hat/hatDataStructure/volatile_engine_benchmark_test.go)
fi

case "$mode" in
  test)
    go test "${focused_files[@]}" -run '^TestVolatileEngine' -count=1
    ;;
  race)
    go test -race "${focused_files[@]}" -run '^TestVolatileEngine' -count=1
    ;;
  vet)
    go vet "${focused_files[@]}"
    ;;
  package)
    go test ./hat/hatDataStructure -count=1
    ;;
  benchmark)
    go test "${focused_files[@]}" -run '^$' -bench '^Benchmark(VolatileEngine|Map)' -benchmem -count=5
    ;;
  format)
    gofmt -w hat/hatDataStructure/volatile_engine.go hat/hatDataStructure/volatile_engine_test.go hat/hatDataStructure/volatile_engine_benchmark_test.go
    ;;
  *)
    printf 'usage: %s test|race|vet|package|benchmark|format\n' "$0" >&2
    exit 2
    ;;
esac
