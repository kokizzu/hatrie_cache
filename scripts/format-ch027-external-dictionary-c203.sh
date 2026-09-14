#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDictionary/dictionary.go hat/hatDictionary/ch027_external_dictionary_test.go hat/hatDictionary/ch027_external_dictionary_benchmark_test.go
