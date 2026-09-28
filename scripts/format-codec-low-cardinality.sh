#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCodec/low_cardinality_dictionary.go \
  hat/hatCodec/low_cardinality_dictionary_test.go \
  hat/hatCodec/low_cardinality_dictionary_benchmark_test.go
