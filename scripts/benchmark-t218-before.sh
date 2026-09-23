#!/usr/bin/env bash
set -euo pipefail
trap 'make cleanup-hatrie-tmp-after-test' EXIT

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT218PrefixScanBaseline$' -benchmem -benchtime=50ms -count=3
