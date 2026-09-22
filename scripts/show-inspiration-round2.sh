#!/usr/bin/env bash
set -euo pipefail

limit="${LIMIT:-25}"
awk -v limit="$limit" '
  /\[ \]/ {
    print
    count++
    if (count >= limit) {
      exit
    }
  }
' INSPIRATION_ROUND2.md
