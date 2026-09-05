#!/usr/bin/env bash
set -euo pipefail

awk '
  /^- \[[ xX]\] / {
    total++
    if ($0 ~ /^- \[[xX]\] /) checked++
  }
  END {
    printf "checked=%d unchecked=%d total=%d\n", checked, total - checked, total
  }
' INSPIRATION.md

awk '
  /^- \[ \] T/ && shown < 120 {
    printf "%d:%s\n", NR, $0
    shown++
  }
' INSPIRATION.md
