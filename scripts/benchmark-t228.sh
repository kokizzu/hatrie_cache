#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT228TupleFormat(UnpackBaseline|Reader(Unpack|ExactUnpack))$' -benchmem -count=5 -cpu=1
