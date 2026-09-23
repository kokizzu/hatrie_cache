#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/c212_hash_join.go hat/hatSql/ch_g04_runtime_bloom_test.go hat/hatSql/ch_g04_runtime_bloom_benchmark_test.go
