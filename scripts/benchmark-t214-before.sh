#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkT214SnapshotTransferBaseline$' -benchmem -count=3
