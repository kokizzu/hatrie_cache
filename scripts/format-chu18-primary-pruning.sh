#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPrimaryPruning/index.go hat/hatPrimaryPruning/index_test.go hat/hatPrimaryPruning/index_benchmark_test.go
