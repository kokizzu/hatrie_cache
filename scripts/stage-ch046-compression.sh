#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CH046_WIRE_COMPRESSION.md \
  ENGINE_IDEAS.md \
  Makefile \
  hat/hatSql/columnar_block_stream.go \
  hat/hatSql/ch046_wire_compression_test.go \
  hat/hatSql/ch046_wire_compression_baseline_benchmark_test.go \
  hat/hatSql/ch046_wire_compression_benchmark_test.go \
  scripts/benchmark-ch046-after.sh \
  scripts/benchmark-ch046-before.sh \
  scripts/format-ch046-compression.sh \
  scripts/race-ch046-compression.sh \
  scripts/review-ch046-compression.sh \
  scripts/stage-ch046-compression.sh \
  scripts/test-ch046-compression.sh \
  scripts/test-ch046-package.sh \
  scripts/vet-ch046-compression.sh
