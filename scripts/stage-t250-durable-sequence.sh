#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md INSPIRATION_ROUND2.md Makefile T250_DURABLE_SEQUENCE.md \
    hat/hatDataStructure/t250_durable_sequence.go \
    hat/hatDataStructure/t250_durable_sequence_test.go \
    hat/hatDataStructure/t250_durable_sequence_benchmark_test.go \
    scripts/benchmark-t250-durable-sequence.sh \
    scripts/commit-t250-durable-sequence.sh \
    scripts/format-t250-durable-sequence.sh \
    scripts/push-t250-durable-sequence.sh \
    scripts/race-t250-durable-sequence.sh \
    scripts/stage-t250-durable-sequence.sh \
    scripts/test-t250-durable-sequence-package.sh \
    scripts/test-t250-durable-sequence.sh \
    scripts/vet-t250-durable-sequence.sh
git diff --cached --check
