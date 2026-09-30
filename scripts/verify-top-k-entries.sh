#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/top_k.go \
  hat/hatDataStructure/top_k_test.go \
  hat/hatDataStructure/top_k_entries_into_test.go
go test -race \
  hat/hatDataStructure/top_k.go \
  hat/hatDataStructure/top_k_test.go \
  hat/hatDataStructure/top_k_entries_into_test.go
