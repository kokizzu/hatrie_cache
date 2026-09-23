#!/usr/bin/env bash
set -euo pipefail

trap 'make cleanup-hatrie-tmp-after-test >/dev/null' EXIT
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkHashIndex(ExactLookup|Build)' -benchmem -benchtime=100ms -count=3
