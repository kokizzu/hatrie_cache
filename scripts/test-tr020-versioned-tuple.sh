#!/bin/sh
set -eu
go test ./hat/hatDataStructure -run 'TestVersionedTuple' -count=1
