#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
rg -n -A 32 -B 8 'SQL|Query|Benchmark' "$root/BENCHMARK.md"
