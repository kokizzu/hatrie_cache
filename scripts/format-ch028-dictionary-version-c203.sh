#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDictionary/dictionary.go hat/hatDictionary/ch028_dictionary_version_test.go hat/hatDictionary/ch028_dictionary_version_benchmark_test.go
