#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
GOCACHE="${GOCACHE:-$root/build/mu039-go-cache}" go test ./hat/hatSql -run '^$' -bench 'BenchmarkMU039Explain' -benchmem -benchtime="${BENCHTIME:-500ms}" -count="${COUNT:-5}"
