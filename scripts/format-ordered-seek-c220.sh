#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/ordered_index.go hat/hatDataStructure/ordered_reverse.go hat/hatDataStructure/ordered_snapshot_cursor.go hat/hatDataStructure/ordered_seek_fastpath_benchmark_test.go hat/hatDataStructure/ordered_seek_fastpath_test.go
