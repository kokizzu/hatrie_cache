#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT(229MemtxUpsertBaseline|230MemtxUpsertOnReplace)$' -benchmem -benchtime=3s -count=3
