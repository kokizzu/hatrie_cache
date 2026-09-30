#!/usr/bin/env bash
set -euo pipefail

go test hat/hatDataStructure/differential_multiset.go \
    hat/hatDataStructure/logical_compaction.go \
    hat/hatDataStructure/logical_compaction_records_benchmark_test.go \
    -run '^$' -bench '^BenchmarkLogicalCompactionRecords$' -benchmem -count=10
