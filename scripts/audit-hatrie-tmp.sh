#!/usr/bin/env bash
set -euo pipefail

printf 'temporary paths matching *hatrie*:\n'
found=0
while IFS= read -r -d '' path; do
  found=1
  printf '%s\t%s\n' "$(stat -c '%F' -- "$path")" "$path"
done < <(find /tmp -mindepth 1 -maxdepth 1 -iname '*hatrie*' -print0 2>/dev/null | sort -z)

[[ "$found" -eq 1 ]] || printf 'none\n'
