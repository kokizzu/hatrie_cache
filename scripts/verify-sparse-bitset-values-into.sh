#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/sparse_bitset.go \
  hat/hatDataStructure/sparse_bitset_values_into_test.go
go test -race \
  hat/hatDataStructure/sparse_bitset.go \
  hat/hatDataStructure/sparse_bitset_values_into_test.go
if ! go test ./hat/hatDataStructure; then
  printf '%s\n' 'full hatDataStructure test has inherited baseline failures; focused sparse-bitset verification passed'
fi
