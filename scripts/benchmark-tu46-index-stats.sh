#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatIndexStats -run '^$' -bench 'Benchmark(DirectIndexCounter|TrackerObserveSampled|TrackerObserveEveryKey|TrackerSnapshot)$' -benchmem -count=5 | tee build/benchmarks/tu46-index-stats.txt
