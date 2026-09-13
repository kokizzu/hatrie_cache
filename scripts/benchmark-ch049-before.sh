#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^$' -bench '^BenchmarkObjectStoreTargetBackup$' -benchmem -count=5
