#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkPersistentDeleteBitmap(VerticalTTL.*|FullRowControl)$' -benchmem -benchtime=200ms -count=5
