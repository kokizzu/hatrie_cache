#!/bin/sh
set -eu

rg -n -C 3 'metrics|EXPLAIN ANALYZE|byte accounting|filtered query' README.md BENCHMARK.md
