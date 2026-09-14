#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^$' -bench '^BenchmarkCH022ObjectStoreIncrementalBackup$' -benchmem -count=5
