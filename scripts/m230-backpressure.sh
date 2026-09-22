#!/usr/bin/env bash
set -euo pipefail

mode="${1:-inspect}"

case "$mode" in
inspect)
  rg -n -i 'frontier|backpressure|checkpoint|Advance|Publish|Subscribe|Stats' hat/hatReplication | head -n 260
  ;;
source)
  sed -n '1,260p' hat/hatReplication/tu39_space_changefeed.go
  sed -n '260,620p' hat/hatReplication/tu39_space_changefeed.go
  ;;
tests)
  sed -n '1,220p' hat/hatReplication/m230_source_backpressure_test.go
  ;;
benchmark)
  go test ./hat/hatReplication -run '^$' -bench 'BenchmarkM230SpaceChangefeed' -benchmem -count=5
  ;;
tail)
  sed -n '620,760p' hat/hatReplication/tu39_space_changefeed.go
  ;;
package)
  go test ./hat/hatReplication -count=1
  ;;
race)
  go test -race ./hat/hatReplication -run 'TestSpaceChangefeedBackpressure' -count=1
  ;;
vet)
  go vet ./hat/hatReplication
  ;;
format)
  gofmt -w hat/hatReplication/m230_source_backpressure.go hat/hatReplication/m230_source_backpressure_test.go hat/hatReplication/m230_source_backpressure_benchmark_test.go
    ;;
  test)
    go test ./hat/hatReplication -run 'TestSpaceChangefeedBackpressure' -count=1
    ;;
  *)
    printf 'unknown M230 mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
