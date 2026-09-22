#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatSql/mz045_arrangement_recommendation_cache.go \
    hat/hatSql/mz045_arrangement_recommendation_cache_test.go \
    hat/hatSql/mz045_arrangement_recommendation_cache_baseline_benchmark_test.go \
    hat/hatSql/mz045_arrangement_recommendation_cache_public_test.go
