#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench 'BenchmarkMZ048' -benchmem -count=5
