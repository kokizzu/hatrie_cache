#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  IDEA_GAP_CATALOG.md \
  Makefile \
  README.md \
  TTG10_WAL_SEGMENT_SEEK.md \
  hat/hatCache/journal.go \
  hat/hatCache/journal_segments.go \
  hat/hatCache/tt_g10_segment_seek_test.go \
  scripts/m229-stage.sh \
  scripts/m229-commit.sh \
  scripts/m229-push.sh \
  scripts/m229-tt-g10-benchmark.sh \
  scripts/m229-tt-g10-format.sh \
  scripts/m229-tt-g10-package-test.sh \
  scripts/m229-tt-g10-race.sh \
  scripts/m229-tt-g10-test.sh \
  scripts/m229-tt-g10-vet.sh
