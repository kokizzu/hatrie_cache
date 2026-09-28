#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatHash/hash.go hat/hatHash/hash_key_fastpath_test.go hat/hatHash/hash_key_fastpath_benchmark_test.go
