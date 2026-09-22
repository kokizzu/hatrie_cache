#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT229MemtxUpsert(Baseline|BeforeReplace)$' -benchmem -count=5
