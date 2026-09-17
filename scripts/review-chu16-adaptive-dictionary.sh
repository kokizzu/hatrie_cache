#!/bin/sh
set -eu

git diff --check
git diff --cached --check
git status --short
rg -n '^## Adaptive Low-Cardinality Storage$' README.md
rg -n '^# Adaptive Low-Cardinality String Storage$' CHU16_ADAPTIVE_LOW_CARDINALITY.md
rg -n '^## CHU16 Adaptive Low-Cardinality String Storage$' BENCHMARK.md
rg -n '^\| CH-U16 \|' PRODUCT_IDEA_GAPS.md
git diff --stat -- BENCHMARK.md CHU16_ADAPTIVE_LOW_CARDINALITY.md Makefile PRODUCT_IDEA_GAPS.md README.md hat/hatSql/chu16_adaptive_dictionary_benchmark_test.go hat/hatSql/chu16_adaptive_dictionary_test.go hat/hatSql/typed_table.go scripts/benchmark-chu16-adaptive-dictionary.sh scripts/commit-chu16-adaptive-dictionary.sh scripts/delivery-chu16-adaptive-dictionary.sh scripts/format-chu16-adaptive-dictionary.sh scripts/push-chu16-adaptive-dictionary.sh scripts/race-chu16-adaptive-dictionary.sh scripts/review-chu16-adaptive-dictionary.sh scripts/stage-chu16-adaptive-dictionary.sh scripts/test-chu16-adaptive-dictionary.sh scripts/test-chu16-package.sh scripts/vet-chu16-adaptive-dictionary.sh
