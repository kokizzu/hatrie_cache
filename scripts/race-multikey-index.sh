#!/usr/bin/env sh
set -eu

printf '%s\n' 'running multikey index race tests'
go test -race -count=1 -run 'TestStringMultikeyIndex' ./hat/hatDataStructure
