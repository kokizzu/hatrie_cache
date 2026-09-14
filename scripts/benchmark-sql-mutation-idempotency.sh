#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkExecuteSQLMutationRetry$' -benchmem -count=5
