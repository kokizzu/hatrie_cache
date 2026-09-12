#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkOrderedIndex(SnapshotCursor|Iterator)Next$|^BenchmarkOrderedIndexUpsert(Without|With)Snapshot$' -benchmem -count=5 -benchtime=500ms
