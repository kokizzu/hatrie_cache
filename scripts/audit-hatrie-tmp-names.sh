#!/usr/bin/env bash
set -euo pipefail

printf 'Top-level /tmp entries with Hatrie-related names\n'
printf 'Protected: /tmp/hatrie-cache-inspiration-next10\n'
found=0
shopt -s nullglob dotglob
for entry in /tmp/*; do
  name=${entry##*/}
  case "$name" in
    *hatrie*|*hatrie_cache*|*hatrie-cache*)
      printf '%s\n' "$entry"
      found=1
      ;;
  esac
done
if (( found == 0 )); then
  printf 'No matching entries\n'
fi

printf 'Recursive matches outside the protected worktree\n'
find /tmp -mindepth 1 -path /tmp/hatrie-cache-inspiration-next10 -prune -o -iname '*hatrie*' -print 2>/dev/null || true
