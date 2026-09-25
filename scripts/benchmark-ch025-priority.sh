#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkCH025(BaselineExplicitPrioritySchedule|PolicyPrioritySchedule)$' -benchmem -count=5
