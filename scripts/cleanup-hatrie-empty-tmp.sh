#!/usr/bin/env bash
set -euo pipefail

plan_file="/tmp/hatrie-empty-tmp-cleanup.plan"
protected_path="/tmp/hatrie-cache-next-goal"

write_plan() {
  : > "$plan_file"
  while IFS= read -r -d '' root; do
    find "$root" -depth -type d -print >> "$plan_file"
  done < <(find /tmp -mindepth 1 -maxdepth 1 -type d -name 'hatrie-cache-*' ! -path "$protected_path" -print0)
}

case "${1:-}" in
preview)
  write_plan
  printf 'Hatrie empty-directory cleanup plan (unprotected trees only):\n'
  if [[ ! -s "$plan_file" ]]; then
    rm -f "$plan_file"
    printf 'Summary: 0 candidate(s); plan removed.\n'
    exit 0
  fi
  while IFS= read -r path; do
    printf '%s\n' "$path"
  done < "$plan_file"
  count=$(wc -l < "$plan_file")
  printf 'Summary: %s candidate(s); plan: %s\n' "$count" "$plan_file"
  ;;
apply)
  if [[ ! -s "$plan_file" ]]; then
    printf 'No cleanup plan found at %s; run preview first.\n' "$plan_file" >&2
    exit 1
  fi
  removed=0
  skipped=0
  while IFS= read -r path; do
    if [[ "$path" == "$protected_path" || "$path" != /tmp/hatrie-cache-* || ! -d "$path" ]]; then
      printf 'skip invalid or missing path: %s\n' "$path" >&2
      skipped=$((skipped + 1))
      continue
    fi
    if rmdir -- "$path" 2>/dev/null; then
      printf 'removed %s\n' "$path"
      removed=$((removed + 1))
    else
      printf 'skip non-empty or changed path: %s\n' "$path"
      skipped=$((skipped + 1))
    fi
  done < "$plan_file"
  rm -f "$plan_file"
  printf 'Summary: %s removed, %s skipped; plan removed.\n' "$removed" "$skipped"
  ;;
*)
  printf 'usage: %s preview|apply\n' "$0" >&2
  exit 2
  ;;
esac
