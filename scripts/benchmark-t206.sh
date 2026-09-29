#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^Benchmark(TU09Bootstrap(AdvanceWAL|Snapshot)|T206Bootstrap(MarshalSnapshot|RestoreSnapshot|Save))$' -benchmem -benchtime=200ms -count=5
