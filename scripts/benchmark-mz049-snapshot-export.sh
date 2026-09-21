#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSnapshotExport$' -benchmem -benchtime=500ms -count=3
