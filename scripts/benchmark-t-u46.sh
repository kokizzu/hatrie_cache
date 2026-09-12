#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkIndexStats' -benchmem -benchtime=250ms -count=5
