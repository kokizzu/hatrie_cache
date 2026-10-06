#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  DATA_STRUCTURE.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU019_DURABLE_TUPLE_FIELD_JOURNAL.md \
  hat/hatCache/command.go \
  hat/hatCache/journal.go \
  hat/hatCache/tu19_tuple_commands.go \
  hat/hatCache/tu19_tuple_field_journal_benchmark_test.go \
  hat/hatCache/tu19_tuple_field_journal_test.go \
  hat/hatDataStructure/tr020_versioned_tuple.go \
  hat/hatDataStructure/tu19_tuple_field_journal_benchmark_test.go \
  hat/hatDataStructure/tu19_tuple_field_journal_test.go \
  scripts/benchmark-tu19.sh \
  scripts/format-tu19.sh \
  scripts/race-tu19.sh \
  scripts/ship-tu19.sh \
  scripts/status-tu19.sh \
  scripts/test-tu19-cache.sh \
  scripts/test-tu19-ds.sh \
  scripts/test-tu19-package.sh \
  scripts/vet-tu19.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: add durable tuple field-operation journal [skip ci]'
git push -u origin "$(git branch --show-current)"
