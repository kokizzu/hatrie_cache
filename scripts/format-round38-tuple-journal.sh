#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/tuple_field_operation_journal.go \
  hat/hatDataStructure/tu19_tuple_field_operation_journal_test.go
