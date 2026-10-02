#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^TestTU23MultiKeyIndex' -count=1
go test -race ./hat/hatDataStructure -run '^TestTU23MultiKeyIndex' -count=1
go test ./hat/hatDataStructure -count=1
go vet ./hat/hatDataStructure
gofmt -d \
  hat/hatDataStructure/multikey_index.go \
  hat/hatDataStructure/string_multikey_index.go \
  hat/hatDataStructure/t_u23_multikey_index_test.go \
  hat/hatDataStructure/t_u23_multikey_baseline_benchmark_test.go \
  hat/hatDataStructure/t_u23_multikey_benchmark_test.go
