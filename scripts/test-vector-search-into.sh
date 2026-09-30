#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatDataStructure/vector.go \
  hat/hatDataStructure/vector_search_into_test.go
