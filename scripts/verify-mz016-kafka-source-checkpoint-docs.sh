#!/usr/bin/env bash
set -euo pipefail

rg -n "MZ-016|checkpoint|KafkaTableSourceCheckpoint|mz-016|MZ016" \
  MZ016_KAFKA_SOURCE_CHECKPOINT.md README.md INSPIRATION_BACKLOG.md BENCHMARK.md
