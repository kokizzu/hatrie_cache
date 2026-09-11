#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkBackupBundleSelectiveBaseline$' -benchmem -benchtime=200ms -count=5
