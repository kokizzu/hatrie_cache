#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/ch025_token_postings_index.go \
  hat/hatDataStructure/ch_u13_phrase_postings_index.go \
  hat/hatDataStructure/ch_u13_phrase_postings_index_test.go \
  hat/hatDataStructure/ch_u13_phrase_postings_index_lifecycle_test.go \
  hat/hatDataStructure/ch_u13_phrase_postings_index_baseline_benchmark_test.go \
  hat/hatDataStructure/ch_u13_phrase_postings_index_benchmark_test.go
