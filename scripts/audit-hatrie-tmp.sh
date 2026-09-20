#!/usr/bin/env bash
set -euo pipefail

tmp_root=${TMPDIR:-/tmp}
declare -A protected_paths=()
while IFS= read -r line; do
  case "$line" in
    "worktree "*)
      protected_paths[${line#worktree }]=1
      ;;
  esac
done < <(git worktree list --porcelain)

is_protected() {
  [[ ${protected_paths[$1]+yes} ]]
}

is_under_protected() {
  local protected
  for protected in "${!protected_paths[@]}"; do
    [[ "$1" == "$protected"/* ]] && return 0
  done
  return 1
}

printf 'Hatrie temporary directory audit\n'
printf 'Protected Git worktrees under %s:\n' "$tmp_root"
for protected in "${!protected_paths[@]}"; do
  [[ "$protected" == "$tmp_root"/* ]] || continue
  printf '%s\n' "$protected"
done

printf 'Matching top-level directories:\n'
found=0
while IFS= read -r -d '' path; do
  found=1
  if is_protected "$path"; then
    status=PROTECTED
  else
    status=review
  fi
  mtime=$(stat -c '%y' "$path")
  size=$(du -sh "$path" | cut -f1)
  printf '%s\t%s\t%s\t%s\n' "$status" "$size" "$mtime" "$path"
done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 -type d -iname '*hatrie*' -print0 | sort -z)

if [[ "$found" -eq 0 ]]; then
  printf '(none)\n'
fi

printf 'Nested Hatrie-named paths under %s (for build/test leftovers):\n' "$tmp_root"
found=0
while IFS= read -r -d '' path; do
  is_under_protected "$path" && continue
  found=1
  printf 'review\t%s\n' "$path"
done < <(find "$tmp_root" -mindepth 2 -maxdepth 3 -iname '*hatrie*' -print0 2>/dev/null | sort -z)

if [[ "$found" -eq 0 ]]; then
  printf '(none)\n'
fi

printf 'Common Go/test temporary directories at %s:\n' "$tmp_root"
found=0
while IFS= read -r -d '' path; do
  is_under_protected "$path" && continue
  is_protected "$path" && continue
  found=1
  mtime=$(stat -c '%y' "$path")
  size=$(du -sh "$path" | cut -f1)
  printf 'review\t%s\t%s\t%s\n' "$size" "$mtime" "$path"
done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 -type d \
  \( -name 'go-build*' -o -name 'Test*' -o -iname '*hatrie*' -o -iname '*hat*' -o -name 'tmp*' -o -name '.tmp*' \) -print0 | sort -z)

if [[ "$found" -eq 0 ]]; then
  printf '(none)\n'
fi
