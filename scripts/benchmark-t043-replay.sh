#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkT042Replay$' -benchmem -benchtime=1s -count=5
