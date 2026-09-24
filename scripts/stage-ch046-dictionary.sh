#!/usr/bin/env bash
set -euo pipefail

paths=(
  BENCHMARK.md
  CH046_NATIVE_WIRE_PROTOCOL.md
  CH046_WIRE_DICTIONARY.md
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  Makefile
  hat/hatSql/columnar_block_stream.go
  hat/hatSql/ch046_wire_dictionary_benchmark_test.go
  hat/hatSql/ch046_wire_dictionary_test.go
  scripts/benchmark-ch046-dictionary-after.sh
  scripts/benchmark-ch046-dictionary-before.sh
  scripts/commit-ch046-dictionary.sh
  scripts/format-ch046-dictionary.sh
  scripts/push-ch046-dictionary.sh
  scripts/race-ch046-dictionary.sh
  scripts/stage-ch046-dictionary.sh
  scripts/test-ch046-all.sh
  scripts/test-ch046-cache.sh
  scripts/test-ch046-dictionary.sh
  scripts/test-ch046-package.sh
  scripts/vet-ch046-dictionary.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git status --short
