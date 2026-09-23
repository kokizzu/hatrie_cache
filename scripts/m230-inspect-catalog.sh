#!/usr/bin/env bash
set -euo pipefail

awk '
  /^## / { section = $0 }
  /^\|/ && $0 !~ /^\|[[:space:]]*:?-+[[:space:]]*\|/ {
    if ($0 !~ /Idea|ID|Status/) {
      print section
      print $0
    }
  }
' IDEA_GAP_CATALOG.md
