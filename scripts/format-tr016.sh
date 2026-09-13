#!/usr/bin/env bash
set -eu

gofmt -w hat/hatCache/storage_pinning.go hat/hatCache/tr016_storage_pinning_test.go hat/hatCache/tr016_storage_pinning_benchmark_test.go
