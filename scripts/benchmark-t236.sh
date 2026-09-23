#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatFiber -run '^$' -bench '^BenchmarkT236' -benchmem -count=5
