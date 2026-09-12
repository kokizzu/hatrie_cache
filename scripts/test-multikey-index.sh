#!/usr/bin/env sh
set -eu

printf '%s\n' 'running multikey index tests'
go test -count=1 -run 'TestStringMultikeyIndex' ./hat/hatDataStructure
