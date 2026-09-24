#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  TT024_POSITIONAL_TEXT_INDEX.md \
  hat/hatSchema/tt024_text_index_persistence.go \
  hat/hatSchema/tt024_text_proximity_index_benchmark_test.go \
  hat/hatSchema/tt024_text_proximity_index_test.go \
  scripts/benchmark-tt024-text-index.sh \
  scripts/format-tt024-text-persistence.sh \
  scripts/stage-tt024-text-persistence.sh

git status --short --untracked-files=all
