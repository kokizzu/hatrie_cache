#!/usr/bin/env bash
set -euo pipefail

git add Makefile BENCHMARK.md INSPIRATION.md COLUMNAR_BLOOM_FILTERS.md hat/hatSql/query.go hat/hatCache/sql_columnar_string_bloom_segment_test.go hat/hatCache/sql_columnar_string_bloom_segment_benchmark_test.go scripts/test-c166-string-bloom-in.sh scripts/benchmark-c166-string-bloom-in.sh scripts/format-c166-string-bloom-in.sh scripts/review-c166-string-bloom-in.sh scripts/commit-c166-string-bloom-in.sh scripts/push-c166-string-bloom-in.sh
git commit -m "Add Bloom pruning for string IN predicates"
