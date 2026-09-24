#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench 'BenchmarkTT027' -benchmem -count=5
