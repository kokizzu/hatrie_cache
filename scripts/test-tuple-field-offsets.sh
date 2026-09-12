#!/usr/bin/env bash
set -eu

go test -count=1 ./hat/hatDataStructure -run 'Test(TupleFieldOffsetCache|PackedTuple)'
