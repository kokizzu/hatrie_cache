#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
go test -C "$root" ./hat/hatPipeline -run '^$' -bench '^BenchmarkFrontierRegistry' -benchmem -count=3
