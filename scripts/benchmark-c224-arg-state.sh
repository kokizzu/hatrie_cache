#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  make cleanup-hatrie-tmp-after-test >/dev/null
}
trap cleanup EXIT

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkC224Arg' -benchmem -count=5
