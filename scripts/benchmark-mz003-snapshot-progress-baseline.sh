#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -tags mz003baseline -run '^$' -bench '^BenchmarkMZ003SnapshotRestoreExisting$' -benchtime=500ms -count=5 -benchmem
