#!/usr/bin/env bash
set -eu

go test -run '^$' -bench 'Benchmark(ConditionalFunctionalIndexUpsert|FunctionalIndexUpsertAllRows)$' -benchmem -count=5 ./hat/hatDataStructure
go test -run '^$' -bench 'Benchmark(ConditionalFunctionalIndexBuild|FunctionalIndexBuildAllRows)$' -benchmem -benchtime=1x -count=5 ./hat/hatDataStructure
