#!/usr/bin/env bash
set -euo pipefail

go test -tags=t213baseline ./hat/hatCache -run '^$' -bench '^BenchmarkT213ScheduledSnapshotBaseline$' -benchmem -benchtime=3s -count=3
