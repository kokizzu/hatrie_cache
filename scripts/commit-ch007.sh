#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_BACKLOG.md BENCHMARK.md TYPED_TABLE_DECOMPRESSED_BLOCK_CACHE.md hat/hatSql/contracts.go hat/hatSql/typed_table.go hat/hatSql/columnar_decompressed_block_cache.go hat/hatSql/columnar_decompressed_block_cache_test.go hat/hatSql/ch007_decompressed_block_cache_test.go hat/hatSql/ch007_decompressed_block_cache_baseline_benchmark_test.go hat/hatSql/ch007_decompressed_block_cache_benchmark_test.go scripts/test-ch007.sh scripts/status-ch007.sh scripts/commit-ch007.sh scripts/push-ch007.sh
git commit -m "feat: add decompressed column block cache"
