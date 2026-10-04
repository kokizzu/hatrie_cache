#!/bin/sh
set -eu

go test -race ./hat/hatDataStructure -run '^TestTupleFieldUpdateJournal' -count=1 "$@"
