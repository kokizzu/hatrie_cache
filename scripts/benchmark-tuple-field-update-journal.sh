#!/bin/sh
set -eu

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTupleFieldUpdateJournal' -benchmem -count=5 "$@"
