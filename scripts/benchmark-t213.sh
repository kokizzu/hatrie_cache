#!/usr/bin/env bash
set -euo pipefail

go test -tags=t213 ./hat/hatCache -run '^$' -bench '^BenchmarkT213ScheduledSnapshot$' -benchmem -benchtime=3s -count=3
