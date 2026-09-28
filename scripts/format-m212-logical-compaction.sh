#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/logical_compaction.go \
  hat/hatDataStructure/m212_logical_compaction_test.go \
  hat/hatDataStructure/m212_logical_compaction_baseline_benchmark_test.go \
  hat/hatDataStructure/m212_logical_compaction_benchmark_test.go
