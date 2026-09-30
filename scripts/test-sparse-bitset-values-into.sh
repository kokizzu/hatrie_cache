#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/sparse_bitset.go \
  hat/hatDataStructure/sparse_bitset_values_into_test.go
