#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
unit)
  go test -timeout 5s -count=10 ./hat/hatFiber -run '^TestT032'
  ;;
package)
  go test -timeout 30s ./hat/hatFiber
  ;;
race)
  go test -timeout 30s -race ./hat/hatFiber -run '^TestT032'
  ;;
vet)
  go vet ./hat/hatFiber
  ;;
format)
  gofmt -w hat/hatFiber/local_test.go hat/hatFiber/local_benchmark_test.go hat/hatFiber/local_baseline_benchmark_test.go
  ;;
baseline)
  repo_root=$(cd "$(dirname "$0")/.." && pwd)
  baseline=/tmp/hatrie-t032-baseline
  if [[ -e "$baseline" ]]; then
    printf 'baseline worktree already exists: %s\n' "$baseline" >&2
    exit 1
  fi
  cleanup() {
    git worktree remove --force "$baseline" >/dev/null 2>&1 || true
  }
  trap cleanup EXIT
  git worktree add --detach "$baseline" 9b8bc93b
  cp "$repo_root/hat/hatFiber/local_baseline_benchmark_test.go" "$baseline/hat/hatFiber/local_baseline_benchmark_test.go"
  (
    cd "$baseline"
    GOMAXPROCS=1 go test -run '^$' -bench '^BenchmarkT032Context' -benchmem -count=5 ./hat/hatFiber | sed 's/[[:space:]]\+$//' | tee "$repo_root/T032_BENCHMARK_BASELINE_RAW.txt"
  )
  ;;
benchmark)
  go test -run '^$' -bench '^BenchmarkT032' -benchmem -count=5 ./hat/hatFiber
  ;;
raw)
  GOMAXPROCS=1 go test -run '^$' -bench '^BenchmarkT032' -benchmem -count=5 ./hat/hatFiber | sed 's/[[:space:]]\+$//' | tee T032_BENCHMARK_RAW.txt
  ;;
full)
  cache=$(mktemp -d /tmp/hatrie-t032-gocache.XXXXXX)
  cleanup() {
    rm -rf "$cache"
  }
  trap cleanup EXIT
  GOCACHE="$cache" timeout 120s go test ./...
  ;;
*)
  printf 'usage: %s {unit|package|race|vet|format|baseline|benchmark|raw|full}\n' "$0" >&2
  exit 2
  ;;
esac
