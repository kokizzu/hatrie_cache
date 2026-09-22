#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/spillable_arrangement.go \
  hat/hatDataStructure/storage_space.go \
  hat/hatDataStructure/storage_space_compaction.go \
  hat/hatDataStructure/t216_storage_space_compaction_test.go \
  hat/hatDataStructure/t216_storage_space_compaction_benchmark_test.go
