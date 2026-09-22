#!/usr/bin/env bash
set -euo pipefail

go test -tags=t214baseline ./hat/hatCache -run '^$' -bench '^BenchmarkT214SnapshotPullBaseline$' -benchmem -benchtime=3s -count=3
