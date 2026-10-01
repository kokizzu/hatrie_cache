#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}
case "$mode" in
  test) go test ./hat/hatCache -run 'TestWritePrometheusSourceHealthMetrics' -count=1 ;;
  benchmark) go test ./hat/hatCache -run '^$' -bench 'BenchmarkWritePrometheusSourceHealthMetrics' -benchmem -count=5 ;;
  race) go test -race ./hat/hatCache -run 'TestWritePrometheusSourceHealthMetrics' -count=1 ;;
  vet) go vet ./hat/hatCache ;;
  *) printf 'unknown mode: %s\n' "$mode" >&2; exit 2 ;;
esac
