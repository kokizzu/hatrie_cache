#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== functional_index.go ====='
awk '{printf "%6d %s\n", NR, $0}' hat/hatDataStructure/functional_index.go
printf '%s\n' '===== bitset_index.go ====='
awk '{printf "%6d %s\n", NR, $0}' hat/hatDataStructure/bitset_index.go
printf '%s\n' '===== multikey_index.go ====='
awk '{printf "%6d %s\n", NR, $0}' hat/hatDataStructure/multikey_index.go
