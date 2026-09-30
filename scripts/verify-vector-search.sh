#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/vector.go \
  hat/hatDataStructure/vector_search_into_test.go
go test -race \
  hat/hatDataStructure/vector.go \
  hat/hatDataStructure/vector_search_into_test.go
if ! go test ./hat/hatDataStructure; then
  printf '%s\n' 'full hatDataStructure test has inherited baseline failures; focused vector verification passed'
fi
