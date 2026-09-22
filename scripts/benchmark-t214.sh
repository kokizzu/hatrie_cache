#!/usr/bin/env bash
set -euo pipefail

go test -tags=t214 ./hat/hatCache -run '^$' -bench '^BenchmarkT214SnapshotStream$' -benchmem -benchtime=3s -count=3
