#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/tuple_update_journal.go \
  hat/hatDataStructure/tu19_tuple_update_journal_test.go \
  hat/hatDataStructure/tu19_tuple_update_journal_benchmark_test.go \
  hat/hatDataStructure/tu19_tuple_update_journal_compare_test.go
