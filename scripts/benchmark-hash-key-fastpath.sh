#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatHash -run '^$' -bench '^BenchmarkFNV1a64(Uint64EncodedBaseline|Uint64Fastpath|Int64Fastpath)$' -benchmem -count=5
