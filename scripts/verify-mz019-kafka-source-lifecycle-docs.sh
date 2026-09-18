#!/usr/bin/env bash
set -euo pipefail

rg -n "MZ-019|lifecycle|PauseWithCheckpoint|KafkaTableSourceLifecycle|mz-019|MZ019" \
  MZ019_KAFKA_SOURCE_LIFECYCLE.md README.md INSPIRATION_BACKLOG.md BENCHMARK.md
