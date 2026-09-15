#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/ordered_index.go hat/hatDataStructure/ordered_upsert_fastpath_test.go
