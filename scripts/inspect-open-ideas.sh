#!/usr/bin/env bash
set -euo pipefail

mode="${1:-OPEN}"

case "$mode" in
  OPEN)
    awk '
      /^## / { section = $0 }
      /^\|/ && $0 !~ /^\|[[:space:]]*:?-+[[:space:]]*\|/ {
        row = tolower($0)
        if (row ~ /open|planned|proposed|backlog|not started|todo/) {
          if (!printed_section && section != "") {
            print section
            printed_section = 1
          }
          print
        }
      }
    ' ENGINE_IDEAS.md
    ;;
  ALL)
    awk '/^\|/ && $0 !~ /^\|[[:space:]]*:?-+[[:space:]]*\|/ { print }' ENGINE_IDEAS.md
    ;;
  *)
    printf 'usage: %s OPEN|ALL\n' "$0" >&2
    exit 2
    ;;
esac
