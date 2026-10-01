#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu62"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU62ArithmeticProjection$' -benchtime=100ms -count=5 -benchmem
