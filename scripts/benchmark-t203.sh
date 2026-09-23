#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkT203LeaderWriteFencing$' -benchmem -count=5
