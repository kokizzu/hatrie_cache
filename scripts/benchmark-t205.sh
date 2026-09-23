#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkT205Metrics' -benchmem -count=5
