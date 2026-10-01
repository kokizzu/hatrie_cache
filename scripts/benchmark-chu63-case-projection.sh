#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu63"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU63CaseProjection$' -benchtime=100ms -count=5 -benchmem
