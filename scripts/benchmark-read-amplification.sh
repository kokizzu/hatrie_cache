#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
go test ./hat/hatMetrics -run '^$' -bench '^BenchmarkReadAmplificationRegistryRecord$' -benchmem -count=5
