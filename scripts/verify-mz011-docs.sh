#!/usr/bin/env bash
set -euo pipefail

rg -n 'MZ-11.*\[x\].*MZ011_PARTITION_OFFSET_FRONTIERS|MZ011_PARTITION_OFFSET_FRONTIERS|mz-011-kafka-style-partition-offset-frontiers|# MZ011' \
  INSPIRATION_BACKLOG.md README.md BENCHMARK.md MZ011_PARTITION_OFFSET_FRONTIERS.md
