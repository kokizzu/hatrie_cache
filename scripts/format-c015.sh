#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatPredicate/simd.go \
	hat/hatPredicate/simd_portable.go \
	hat/hatPredicate/simd_amd64.go \
	hat/hatPredicate/simd_test.go \
	hat/hatPredicate/simd_benchmark_test.go
