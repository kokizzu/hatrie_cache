#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPipeline/async_insert_dedup.go hat/hatPipeline/async_insert_dedup_test.go hat/hatPipeline/async_insert_dedup_security_test.go hat/hatPipeline/async_insert_dedup_expiry_test.go hat/hatPipeline/async_insert_dedup_corruption_test.go hat/hatPipeline/async_insert_dedup_benchmark_test.go
