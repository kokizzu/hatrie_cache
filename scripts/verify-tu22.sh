#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestTU22CrossIndexUniqueSet' -count=1
go test -race ./hat/hatDataStructure -run '^TestTU22CrossIndexUniqueSet' -count=1
go test ./hat/hatDataStructure -count=1
go vet ./hat/hatDataStructure
gofmt -d \
  hat/hatDataStructure/t_u22_cross_index_unique.go \
  hat/hatDataStructure/t_u22_cross_index_unique_test.go \
  hat/hatDataStructure/t_u22_cross_index_unique_benchmark_test.go \
  hat/hatDataStructure/t_u22_cross_index_unique_comparison_benchmark_test.go \
  hat/hatDataStructure/t_u22_cross_index_unique_baseline_benchmark_test.go
