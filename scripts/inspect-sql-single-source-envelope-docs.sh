#!/bin/sh
set -eu

rg -n -C 3 'execution arena|row envelope|columnar.*row|SQL Materialized' README.md BENCHMARK.md
