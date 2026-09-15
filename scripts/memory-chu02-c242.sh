#!/usr/bin/env bash
set -euo pipefail

benchmark_root=.
temporary_root=""
measurement_root="$(mktemp -d)"
cleanup() {
  if [[ -n "$temporary_root" ]]; then
    rm -rf "$temporary_root"
  fi
  rm -rf "$measurement_root"
}
trap cleanup EXIT

if [[ ! -f hat/hatSql/asof_join.go ]]; then
  temporary_root="$(mktemp -d)"
  cp -a go.mod go.sum hat "$temporary_root/"
  git show HEAD:hat/hatSql/asof_join.go > "$temporary_root/hat/hatSql/asof_join.go"
  benchmark_root="$temporary_root"
fi

(cd "$benchmark_root" && go test -c -o "$measurement_root/hatSql.test" ./hat/hatSql)

run_memory_sample() {
  local label="$1"
  local pattern="$2"
  printf '%s\n' "--- $label ---"
  (cd "$benchmark_root" && /usr/bin/time -v "$measurement_root/hatSql.test" \
    -test.run '^$' \
    -test.bench "$pattern" \
    -test.benchtime=1x \
    -test.benchmem \
    -test.count=1)
}

run_memory_sample "materialized baseline" '^BenchmarkCHU02ExternalOrderByBaselineAndStreaming/materialized_baseline$'
run_memory_sample "streaming external spill" '^BenchmarkCHU02ExternalOrderByBaselineAndStreaming/streaming_external_spill$'
