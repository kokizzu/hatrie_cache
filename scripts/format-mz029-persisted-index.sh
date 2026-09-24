#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/spillable_arrangement.go hat/hatDataStructure/spillable_arrangement_index.go hat/hatDataStructure/mz029_persisted_index_test.go hat/hatDataStructure/mz029_persisted_index_internal_test.go
