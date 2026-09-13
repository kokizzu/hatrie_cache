#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkRemotePartCache$' -benchmem -count=5
