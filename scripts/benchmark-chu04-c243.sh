#!/usr/bin/env bash
set -euo pipefail

benchmark_root=.
temporary_root=""
cleanup() {
  if [[ -n "$temporary_root" ]]; then
    rm -rf "$temporary_root"
  fi
}
trap cleanup EXIT

if [[ ! -f hat/hatSql/asof_join.go ]]; then
  temporary_root="$(mktemp -d)"
  cp -a go.mod go.sum hat "$temporary_root/"
  git show HEAD:hat/hatSql/asof_join.go > "$temporary_root/hat/hatSql/asof_join.go"
  benchmark_root="$temporary_root"
fi

(cd "$benchmark_root" && go test ./hat/hatSql \
  -run '^$' \
  -bench '^BenchmarkCHU04ExternalDistinctBaselineAndStreaming$' \
  -benchmem \
  -count=5)
