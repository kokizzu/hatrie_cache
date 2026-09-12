#!/usr/bin/env bash
set -eu

go test -run '^$' -bench 'Benchmark(U64AntichainBuild|NaiveAntichainBuild)$' -benchmem -benchtime=1x -count=5 ./hat/hatDataStructure
