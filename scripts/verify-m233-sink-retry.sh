#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  package)
    go test ./hat/hatReplication -count=1
    ;;
  race)
    go test -race ./hat/hatReplication -count=1
    ;;
  vet)
    go vet ./hat/hatReplication
    ;;
  benchmark)
    go test ./hat/hatReplication -run '^$' -bench 'BenchmarkSinkRetryQueue' -benchmem -count=5
    ;;
  docs)
    test -s M233_SINK_RETRY_OUTBOX.md
    rg -n 'M233_SINK_RETRY_OUTBOX.md|## M233 Sink Retry Outbox' INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md M233_SINK_RETRY_OUTBOX.md
    ;;
  staged)
    git diff --cached --check
    git diff --cached --stat
    git status --short
    ;;
  *)
    echo "usage: $0 package|race|vet|benchmark|docs|staged" >&2
    exit 2
    ;;
esac
