#!/bin/sh
set -eu

awk '
  /^- \[ \]/ {
    print NR ":" $0
    count++
    if (count >= 40) {
      exit
    }
  }
' INSPIRATION.md
