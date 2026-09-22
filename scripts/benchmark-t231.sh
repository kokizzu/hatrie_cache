#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT(229MemtxUpsertBaseline|231MemtxUpsertAfterReplace)$' -benchmem -benchtime=3s -count=3
