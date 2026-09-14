#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test ./hat/hatSql ./hat/hatCache -count=1
go test -race ./hat/hatCache -run '^TestHatTrieSQLColumnarStringBloomSegment' -count=1
go vet ./hat/hatSql ./hat/hatCache
test -f COLUMNAR_BLOOM_FILTERS.md
rg -n -F 'C166' INSPIRATION.md
rg -n -F 'ClickHouse-style string Bloom literal IN' BENCHMARK.md
rg -n -F 'COLUMNAR_BLOOM_FILTERS.md' INSPIRATION.md
git status --short -- Makefile BENCHMARK.md INSPIRATION.md COLUMNAR_BLOOM_FILTERS.md hat/hatSql/query.go hat/hatCache/sql_columnar_string_bloom_segment_test.go hat/hatCache/sql_columnar_string_bloom_segment_benchmark_test.go scripts/test-c166-string-bloom-in.sh scripts/benchmark-c166-string-bloom-in.sh scripts/format-c166-string-bloom-in.sh scripts/review-c166-string-bloom-in.sh scripts/commit-c166-string-bloom-in.sh scripts/push-c166-string-bloom-in.sh
