#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatReplication -run '^$' -bench '^BenchmarkM033DGlobalTimestampSnapshot' -benchmem -count=5
