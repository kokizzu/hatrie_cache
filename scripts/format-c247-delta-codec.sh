#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/delta_uint64_codec.go \
  hat/hatDataStructure/delta_uint64_codec_test.go \
  hat/hatDataStructure/delta_uint64_codec_benchmark_test.go \
  hat/hatDataStructure/gorilla_float64_codec.go \
  hat/hatDataStructure/gorilla_float64_codec_test.go
