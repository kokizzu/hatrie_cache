#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^$' -bench '^BenchmarkPlanBackupChain$' -benchtime=200ms -benchmem -count=5
