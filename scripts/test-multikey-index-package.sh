#!/usr/bin/env sh
set -eu

printf '%s\n' 'running complete multikey index package tests'
go test -count=1 ./hat/hatDataStructure
