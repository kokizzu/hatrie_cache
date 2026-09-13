#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH050JSONRowsDecode$' -benchmem -count=5
