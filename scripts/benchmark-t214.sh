#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkT214SnapshotTransferModes$' -benchmem -count=3
