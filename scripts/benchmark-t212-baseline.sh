#!/usr/bin/env bash
set -euo pipefail

go test -tags=t212baseline ./hat/hatCache -run '^$' -bench '^BenchmarkT212' -benchmem -count=5
