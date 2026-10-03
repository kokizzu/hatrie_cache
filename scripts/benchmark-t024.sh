#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkT024ConditionalSpaceIndex$' -benchmem -count=5
