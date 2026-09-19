#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatWorkload/admission.go hat/hatWorkload/admission_test.go hat/hatWorkload/admission_benchmark_test.go
