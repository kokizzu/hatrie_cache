#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^$' -bench 'BenchmarkPlanBackup' -benchmem -count=5
