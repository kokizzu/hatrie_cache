#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/compact_posting.go \
  hat/hatDataStructure/compact_posting_test.go \
  hat/hatDataStructure/compact_posting_benchmark_test.go \
  hat/hatDataStructure/functional_index.go \
  hat/hatDataStructure/hash_index.go \
  hat/hatDataStructure/multikey_index.go \
  hat/hatDataStructure/conditional_index.go
