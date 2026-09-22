#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  make cleanup-hatrie-tmp-after-test >/dev/null
}
trap cleanup EXIT

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC234' -benchmem -count=5
