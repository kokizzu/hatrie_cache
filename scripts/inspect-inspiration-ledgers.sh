#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Inspiration and proposal documents:'
matched=0
while IFS= read -r path; do
  case "$path" in
    *IDEA*|*INSPIR*|*PROPOSAL*|*BENCHMARK*)
      printf '%s\n' "$path"
      matched=1
      ;;
  esac
done < <(rg --files -g '*.md' | sort)

if test "$matched" -eq 0; then
  printf '%s\n' 'none'
fi

printf '%s\n' '' 'Open checklist entries:'
if ! rg -n '^[-*] \[ \]|^[-*] \[-\]|\| \[ \] |\| \[-\] ' \
  INSPIRATION.md INSPIRATION_BACKLOG.md INSPIRATION_ROUND2.md PRODUCT_IDEA_GAPS.md; then
  printf '%s\n' 'none'
fi

printf '%s\n' '' 'Candidate status signals:'
if ! rg -n -i 'grace.?hash|external sort|memory.?overcommit|cpu.?time|query profiler|backup database|chunk dedup|incremental window|delta.?join|materialized result cache|frontier' \
  INSPIRATION_ROUND2.md INSPIRATION_BACKLOG.md ADOPTED_QUERY_ENGINE_IDEAS.md ENGINE_IDEAS.md; then
  printf '%s\n' 'none'
fi
