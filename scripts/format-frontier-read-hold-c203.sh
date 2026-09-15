#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/frontier_read_hold.go hat/hatDataStructure/frontier_read_hold_test.go hat/hatDataStructure/frontier_read_hold_benchmark_test.go
