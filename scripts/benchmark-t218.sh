#!/usr/bin/env bash
set -euo pipefail
trap 'make cleanup-hatrie-tmp-after-test' EXIT

go test -tags=t218 ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT218(PrefixScanBaseline|MultiPartTreePrefixScan)$' -benchmem -benchtime=50ms -count=3
