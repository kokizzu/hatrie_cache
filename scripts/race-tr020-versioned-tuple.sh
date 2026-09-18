#!/bin/sh
set -eu
go test -race ./hat/hatDataStructure -run 'TestVersionedTuple' -count=1
