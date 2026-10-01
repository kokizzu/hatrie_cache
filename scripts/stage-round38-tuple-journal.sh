#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU19_TUPLE_OPERATION_JOURNAL.md \
  hat/hatDataStructure/tuple_field_operation_journal.go \
  hat/hatDataStructure/tu19_tuple_field_operation_journal_test.go \
  scripts/format-round38-tuple-journal.sh \
  scripts/race-round38-tuple-journal.sh \
  scripts/test-round38-tuple-journal.sh \
  scripts/verify-round38-tuple-journal-docs.sh \
  scripts/vet-round38-tuple-journal.sh \
  scripts/review-round38-tuple-journal.sh \
  scripts/stage-round38-tuple-journal.sh \
  scripts/commit-round38-tuple-journal.sh \
  scripts/push-round38-tuple-journal.sh
