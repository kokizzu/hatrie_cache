#!/usr/bin/env bash
set -euo pipefail

rg -n "MZ-013|Debezium|DebeziumKafka|mz-013|MZ013" \
  MZ013_DEBEZIUM_KAFKA_DECODER.md README.md INSPIRATION_BACKLOG.md BENCHMARK.md
