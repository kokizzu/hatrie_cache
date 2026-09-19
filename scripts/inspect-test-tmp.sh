#!/usr/bin/env bash
set -euo pipefail

tmp_root=${TMPDIR:-/tmp}
now=$(date +%s)

printf 'Top-level directories under %s (read-only audit):\n' "$tmp_root"
printf '%-8s %-12s %-8s %s\n' 'AGE_H' 'MODIFIED' 'SIZE' 'PATH'

find "$tmp_root" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\0' \
  | sort -z -n \
  | while IFS= read -r -d '' entry; do
      mtime=${entry%% *}
      path=${entry#* }
      mtime_seconds=${mtime%%.*}
      age_hours=$(( (now - mtime_seconds) / 3600 ))
      modified=$(date -d "@$mtime_seconds" '+%Y-%m-%d %H:%M')
      size='unreadable'
      if size=$(du -sh -- "$path" 2>/dev/null | cut -f1); then
        :
      fi
      case "$path" in
        *hatrie*|*test*|*go-build*|*bench*|*sql*|*redis*|*tarantool*|*cache*)
          marker='likely-test'
          ;;
        *)
          marker='other'
          ;;
      esac
      printf '%-8s %-12s %-8s %s [%s]\n' "$age_hours" "$modified" "$size" "$path" "$marker"
    done
