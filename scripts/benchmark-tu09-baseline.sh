#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench '^BenchmarkSnapshotJoinDirectSequenceControl$' -benchmem -count=5 ./hat/hatBackup
