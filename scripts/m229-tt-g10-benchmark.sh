#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTTG10ReplayExistingFullSegmentScan$' -benchmem -count=5
