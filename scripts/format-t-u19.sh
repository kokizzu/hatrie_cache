#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/tuple_field_update_journal.go \
  hat/hatDataStructure/t_u19_tuple_update_journal_test.go \
  hat/hatDataStructure/t_u19_tuple_update_journal_baseline_benchmark_test.go \
  hat/hatDataStructure/t_u19_tuple_update_journal_benchmark_test.go
