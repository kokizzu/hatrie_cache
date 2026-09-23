#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkT213ScheduledSnapshotBaseline$' -benchmem -count=3
