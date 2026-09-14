#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkExecuteSQLMutation(SetString|OnConflictNothingHit|OnConflictUpdateHit)$' -benchmem -count=5
