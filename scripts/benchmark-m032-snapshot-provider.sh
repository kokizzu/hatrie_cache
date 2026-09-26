#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM032MultiSourceSnapshot(LiveResolveBaseline|BeginAndResolve|PinnedResolve)$' -benchmem -count=5
