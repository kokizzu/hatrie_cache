#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench 'Benchmark(TupleFormatPack|NewPackedTupleBaseline)' -benchmem -count=5
