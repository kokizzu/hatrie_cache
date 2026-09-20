#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^$' -bench 'BenchmarkSnapshotJoin' -benchmem -count=5
