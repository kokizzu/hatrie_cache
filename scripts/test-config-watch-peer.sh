#!/bin/sh
set -eu

mode=${1:-test}
case "$mode" in
  test)
    go test ./hat/hatTopology -run '^TestConfigWatchPeer' -count=1
    ;;
  package)
    go test ./hat/hatTopology -count=1
    ;;
  race)
    go test -race ./hat/hatTopology -run '^TestConfigWatchPeer' -count=1
    ;;
  vet)
    go vet ./hat/hatTopology
    ;;
  benchmark)
    go test ./hat/hatTopology -run '^$' -bench '^BenchmarkConfigWatchPeer' -benchtime=100ms -count=5 -benchmem
    ;;
  verify)
    go test ./hat/hatTopology -run '^TestConfigWatchPeer' -count=1
    go test -race ./hat/hatTopology -run '^TestConfigWatchPeer' -count=1
    go vet ./hat/hatTopology
    ;;
  *)
    printf 'unknown config-watch-peer test mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
