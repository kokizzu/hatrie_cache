#!/usr/bin/env sh
set -eu

printf '%s\n' 'running multikey index vet'
go vet ./hat/hatDataStructure
