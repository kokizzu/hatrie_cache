#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatWorkload/c231_memory_admission.go \
	hat/hatWorkload/c231_memory_admission_test.go \
	hat/hatWorkload/c231_memory_admission_baseline_benchmark_test.go \
	hat/hatWorkload/c231_memory_admission_benchmark_test.go
