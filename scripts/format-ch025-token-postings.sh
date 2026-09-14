#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/ch025_token_postings_index.go \
  hat/hatDataStructure/ch025_token_postings_index_test.go \
  hat/hatDataStructure/ch025_token_postings_index_benchmark_test.go \
  hat/hatDataStructure/ch025_token_postings_index_public_test.go
