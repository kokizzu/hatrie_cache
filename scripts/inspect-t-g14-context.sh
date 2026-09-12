#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== tuple-related files ====='
for file in hat/hatDataStructure/*tuple*.go; do
  printf '%s\n' "--- $file ---"
  awk '{printf "%6d %s\n", NR, $0}' "$file"
done
