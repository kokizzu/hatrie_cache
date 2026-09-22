#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
candidate=${1:-}

print_open_items() {
  local file=$1
  local label=$2

  printf '%s\n' "$label"
  if [[ ! -f "$file" ]]; then
    printf '  missing: %s\n' "$file"
    return
  fi

  awk '
    /^## / { section=$0 }
    /^- \[ \]/ || /^- \[-\]/ {
      printf "  %s | %s\n", section, $0
      found=1
    }
    END { if (!found) print "  none" }
  ' "$file"
}

printf 'Inspiration backlog audit\n'
printf 'Root: %s\n\n' "$root"
print_open_items "$root/INSPIRATION_BACKLOG.md" 'Primary backlog items not marked complete:'
printf '\n'
print_open_items "$root/INSPIRATION_ROUND2.md" 'Round 2 items not marked complete:'

if [[ -n "$candidate" ]]; then
  printf '\nReferences for %s:\n' "$candidate"
  rg -n --hidden --glob '!\.git/**' --glob '!vendor/**' --fixed-strings "$candidate" "$root"
fi
