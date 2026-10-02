#!/usr/bin/env bash
set -euo pipefail

go vet \
  ./hat/hatDataStructure/spillable_arrangement.go \
  ./hat/hatDataStructure/spillable_arrangement_test.go \
  ./hat/hatDataStructure/spillable_arrangement_benchmark_test.go \
  ./hat/hatDataStructure/spillable_arrangement_compaction_benchmark_test.go
