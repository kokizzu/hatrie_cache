#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md IDEA_GAP_CATALOG.md \
  TTG11_WAL_SEGMENT_COMPRESSION.md \
  hat/hatJournal/compress.go hat/hatJournal/journal.go \
  hat/hatCache/journal_zstd_segment_benchmark_test.go \
  hat/hatCache/tt_g11_default_compression_test.go \
  scripts/m230-inspect-catalog.sh \
  scripts/m231-tt-g11-test.sh scripts/m231-tt-g11-benchmark.sh \
  scripts/m231-tt-g11-zstd-test.sh scripts/m231-tt-g11-format.sh \
  scripts/m231-tt-g11-race.sh scripts/m231-tt-g11-vet.sh \
  scripts/m231-tt-g11-package-test.sh scripts/m231-tt-g11-docs.sh \
  scripts/m231-stage.sh scripts/m231-commit.sh scripts/m231-push.sh

git diff --cached --check
git diff --cached --stat
