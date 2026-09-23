#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatDataStructure/space.go hat/hatDataStructure/t215_space_policy_test.go hat/hatDataStructure/t215_space_policy_baseline_benchmark_test.go hat/hatDataStructure/t215_space_policy_benchmark_test.go
