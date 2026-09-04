#!/bin/sh
set -eu

go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkTokenBloomFilter' -benchmem -count=5 -benchtime=250ms
