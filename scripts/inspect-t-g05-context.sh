#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== ordered/index files ====='
rg --files hat/hatDataStructure | rg -i 'ordered|index|tree|btree'
printf '%s\n' '===== ordered/index APIs ====='
rg -n -i 'type .*Index|type .*Iterator|func .*Seek|func .*First|func .*Next|LowerBound|UpperBound' hat/hatDataStructure hat/hatSql --glob '*.go'
