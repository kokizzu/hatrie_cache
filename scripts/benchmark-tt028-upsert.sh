#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTT028(LegacyManualMerge|Upsert)$' -benchmem -count=5
