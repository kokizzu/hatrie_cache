#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU09SnapshotJoin' -benchmem -benchtime=2s -count=3
