#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
test)
  go test ./hat/hatReplication -run '^TestTU39SpaceChangefeed' -count=1
  ;;
package)
  go test ./hat/hatReplication -count=1
  ;;
race)
  go test -race ./hat/hatReplication -run '^TestTU39SpaceChangefeed' -count=1
  ;;
vet)
  go vet ./hat/hatReplication
  ;;
format)
  gofmt -w hat/hatReplication/tu39_space_changefeed.go hat/hatReplication/t_u39_space_changefeed_test.go hat/hatReplication/t_u39_space_changefeed_benchmark_test.go
  ;;
benchmark)
  go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU39' -benchmem -count=5
  ;;
verify)
  test -f TU039_SPACE_CHANGEFEED.md
  rg -q 'TU039_SPACE_CHANGEFEED.md' README.md PRODUCT_IDEA_GAPS.md BENCHMARK.md
  rg -q 'T-U39 Named-Space Changefeed' BENCHMARK.md
  git diff --check
  ;;
*)
  printf 'usage: %s test|package|race|vet\n' "$0" >&2
  exit 2
  ;;
esac
