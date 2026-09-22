#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
  format)
    gofmt -w hat/hatReplication/m227_snapshot_boundary.go hat/hatReplication/m227_snapshot_boundary_test.go
    ;;
  test)
    go test ./hat/hatReplication -run 'TestChangefeedSnapshotBoundary' -count=1
    ;;
  benchmark)
    go test ./hat/hatReplication -run '^$' -bench 'BenchmarkChangefeedSnapshotBoundary' -benchmem -count=5
    ;;
  size)
    go test ./hat/hatReplication -run '^TestChangefeedSnapshotBoundaryBinaryIsSmallerThanJSON$' -v -count=1
    ;;
  race)
    go test -race ./hat/hatReplication -run 'TestChangefeedSnapshotBoundary' -count=1
    ;;
  vet)
    go vet ./hat/hatReplication
    ;;
  package)
    go test ./hat/hatReplication -count=1
    ;;
  docs)
    rg -q 'M227_ATOMIC_SNAPSHOT_FRONTIER.md' INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
    rg -q '## M227' M227_ATOMIC_SNAPSHOT_FRONTIER.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
    ;;
  *)
    printf 'unknown M227 mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
