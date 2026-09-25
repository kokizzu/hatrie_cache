#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkRestoreBackupBundleResumeCheckpoint$' -benchmem -benchtime=10x -count=5
