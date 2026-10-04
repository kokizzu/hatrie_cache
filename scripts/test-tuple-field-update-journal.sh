#!/bin/sh
set -eu

go test ./hat/hatDataStructure -run '^TestTupleFieldUpdateJournal' -count=1 "$@"
