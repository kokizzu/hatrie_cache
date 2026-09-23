#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDictionary/dictionary.go \
  hat/hatDictionary/ch046_dictionary_negative_cache_test.go \
  hat/hatDictionary/ch046_dictionary_negative_cache_options_test.go \
  hat/hatDictionary/ch046_dictionary_negative_cache_benchmark_test.go
