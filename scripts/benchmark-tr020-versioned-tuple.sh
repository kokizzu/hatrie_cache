#!/bin/sh
set -eu
go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkTR020(Baseline|Versioned)' -benchmem -count=5
