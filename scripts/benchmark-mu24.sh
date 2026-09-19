#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatWorkload -run=NONE -bench='Benchmark(DirectWorkloadCounter|Admission)' -benchmem -count=5 | tee build/benchmarks/mu24-admission.txt
