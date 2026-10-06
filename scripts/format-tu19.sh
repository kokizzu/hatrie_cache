#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatCache/command.go \
  hat/hatCache/journal.go \
  hat/hatCache/tu19_tuple_commands.go \
  hat/hatCache/tu19_tuple_field_journal_benchmark_test.go \
  hat/hatCache/tu19_tuple_field_journal_test.go \
  hat/hatDataStructure/tu19_tuple_field_journal_benchmark_test.go \
  hat/hatDataStructure/tr020_versioned_tuple.go \
  hat/hatDataStructure/tu19_tuple_field_journal_test.go
