#!/bin/sh
set -eu

git add -- BENCHMARK.md CHU16_ADAPTIVE_LOW_CARDINALITY.md Makefile PRODUCT_IDEA_GAPS.md README.md hat/hatSql/chu16_adaptive_dictionary_benchmark_test.go hat/hatSql/chu16_adaptive_dictionary_test.go hat/hatSql/typed_table.go scripts/benchmark-chu16-adaptive-dictionary.sh scripts/commit-chu16-adaptive-dictionary.sh scripts/delivery-chu16-adaptive-dictionary.sh scripts/format-chu16-adaptive-dictionary.sh scripts/race-chu16-adaptive-dictionary.sh scripts/review-chu16-adaptive-dictionary.sh scripts/stage-chu16-adaptive-dictionary.sh scripts/test-chu16-adaptive-dictionary.sh scripts/test-chu16-package.sh scripts/vet-chu16-adaptive-dictionary.sh
