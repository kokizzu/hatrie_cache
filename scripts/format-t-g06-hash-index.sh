#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/hash_index.go hat/hatDataStructure/hash_index_test.go hat/hatDataStructure/hash_index_public_test.go hat/hatDataStructure/hash_index_benchmark_test.go
