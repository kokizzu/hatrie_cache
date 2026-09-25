#!/usr/bin/env bash
set -euo pipefail

mode=${1:-preview}
if [[ "$mode" != preview && "$mode" != apply ]]; then
  printf 'usage: %s [preview|apply]\n' "$0" >&2
  exit 2
fi

now=$(date +%s)
protected_root=$(pwd -P)
plan=$(mktemp /tmp/hatrie-tmp-audit.XXXXXX)
trap 'rm -f "$plan"' EXIT

is_live_worktree() {
  local path=$1
  local cwd resolved
  for cwd in /proc/[0-9]*/cwd; do
    [[ -e "$cwd" ]] || continue
    resolved=$(readlink -f "$cwd" 2>/dev/null || true)
    [[ "$resolved" == "$path" ]] && return 0
  done
  return 1
}

format_entry() {
  local path=$1
  local mtime age size status
  mtime=$(stat -c %Y "$path")
  age=$((now - mtime))
  size=$(du -sh "$path" 2>/dev/null | cut -f1)
  status=CANDIDATE
  [[ "$path" == "$protected_root" || "$path" == "$protected_root/"* ]] && status=ACTIVE-WORKTREE
  [[ -e "$path/.git" ]] && status=GIT-PROTECTED
  is_live_worktree "$path" && status=LIVE-CWD
  printf '%-16s age=%-8ss size=%-8s path=%s\n' "$status" "$age" "$size" "$path"
}

printf 'Recursive Hatrie-related temporary inventory under /tmp:\n'
printf 'Protected: active worktree, any directory containing .git, and live process working directories.\n'
printf 'Review-only: generic go-build*/go-link* paths are listed but never removed.\n'

while IFS= read -r path; do
  [[ -d "$path" ]] || continue
  format_entry "$path"
  if [[ "$path" == *hatrie* && "$path" != "/tmp/hatrie-cache-inspiration-next10" && ! -d "$path/.git" ]] && ! is_live_worktree "$path"; then
    printf '%s\n' "$path" >> "$plan"
  fi
done < <(find /tmp -mindepth 1 -maxdepth 3 \( -path "$protected_root" -o -path "$protected_root/*" \) -prune -o -type d \( -iname '*hatrie*' -o -name 'go-build*' -o -name 'go-link*' \) -print 2>/dev/null | sort)

count=$(wc -l < "$plan")
printf 'Removal candidates: %s\n' "$count"

if [[ "$mode" == preview ]]; then
  if (( count > 0 )); then
    printf 'Preview plan:\n'
    while IFS= read -r path; do
      printf 'REMOVE %s\n' "$path"
    done < "$plan"
  else
    printf 'Plan: none\n'
  fi
  exit 0
fi

if (( count == 0 )); then
  printf 'Nothing to remove.\n'
  exit 0
fi

while IFS= read -r path; do
  [[ -n "$path" ]] || continue
  rm -rf -- "$path"
  printf 'REMOVED %s\n' "$path"
done < "$plan"
