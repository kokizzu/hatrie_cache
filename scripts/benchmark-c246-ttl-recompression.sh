#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTTLRecompression' -benchmem -benchtime=200ms -count=5
