#!/usr/bin/env bash
set -eu

go test -run '^$' -bench 'Benchmark(TupleFieldOffsetCacheAccess|TupleFieldScan)$' -benchmem -benchtime=200ms -count=5 ./hat/hatDataStructure
go test -run '^$' -bench 'Benchmark(NewPackedTuple|NaiveTupleCopies)$' -benchmem -benchtime=200ms -count=5 ./hat/hatDataStructure
