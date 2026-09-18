#!/bin/sh
set -eu
go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkTR020Baseline' -benchmem -count=5
