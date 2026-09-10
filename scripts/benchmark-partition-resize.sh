#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPartition -run '^$' -bench '^BenchmarkPartition(Index|ResizePlanTarget|ResizePlanRoute|ResizePlanTwoLookups)$' -benchmem -count=5
