#!/usr/bin/env bash
set -euo pipefail

protected_path="/tmp/hatrie-cache-next-goal"
found=0
while IFS= read -r -d '' path; do
  [[ "$path" == "$protected_path" ]] && continue
  found=1
  printf '%s\n' "$path"
  find "$path" -maxdepth 8 -mindepth 1 -printf '  %y %p\n'
done < <(find /tmp -mindepth 1 -maxdepth 1 -type d -iname '*hatrie*' -print0)

if [[ "$found" -eq 0 ]]; then
  printf '(none)\n'
fi
