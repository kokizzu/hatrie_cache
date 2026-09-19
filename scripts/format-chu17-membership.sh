#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatMembership/uint64_set.go hat/hatMembership/uint64_set_test.go hat/hatMembership/uint64_set_benchmark_test.go
