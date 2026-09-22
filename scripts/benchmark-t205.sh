#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkT205(ObserveExistingTarget|SnapshotOneTarget)$' -benchmem -count=5
