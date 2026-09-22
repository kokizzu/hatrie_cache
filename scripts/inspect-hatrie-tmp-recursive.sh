#!/usr/bin/env bash
set -euo pipefail

tmp_root=${TMPDIR:-/tmp}

printf 'Recursive Hatrie-related inventory under %s\n' "$tmp_root"
printf 'Protected paths are reported but never modified.\n'

found=0
report_path() {
  local path=$1
  local kind=$2
  size=$(du -sh -- "$path" 2>/dev/null | cut -f1 || printf '?')
  age=$(stat -c '%Y' -- "$path" 2>/dev/null || printf '0')
  now=$(date +%s)
  age_seconds=$((now - age))
  printf '%-19s age=%-8ss size=%-8s path=%s\n' "$kind" "$age_seconds" "$size" "$path"
}

while IFS= read -r top_level; do
  if [[ -e "$top_level/.git" ]]; then
    report_path "$top_level" 'ACTIVE-WORKTREE'
    found=1
    continue
  fi

  while IFS= read -r path; do
    report_path "$path" 'REVIEW-ONLY'
    found=1
  done < <(
    find "$top_level" -mindepth 0 -maxdepth 3 -type d \( \
      -iname '*hatrie*' -o \
      -iname 'go-build*' -o \
      -iname '*test-tmp*' \
    \) -print 2>/dev/null | sort
  )
done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 -type d -print 2>/dev/null | sort)

if ((found == 0)); then
  printf 'No Hatrie-related or generic Go build/test directories found.\n'
fi
