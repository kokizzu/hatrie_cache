#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -run '^$' -bench '^BenchmarkMZ003SnapshotRestore(Existing|WithProgress)$' -benchtime=500ms -count=5 -benchmem
