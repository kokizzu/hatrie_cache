#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkC220OrderedIndex(Seek|SnapshotSeek|ReverseSeek|ReverseSnapshotSeek)$' -benchmem -benchtime=200ms -count=3
