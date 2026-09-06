#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
go test ./hat/hatCodec -run '^$' -bench BenchmarkEncodeBitPackedUint64 -benchmem -count=1
