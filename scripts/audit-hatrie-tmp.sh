#!/usr/bin/env bash
set -euo pipefail

tmp_root=${TMPDIR:-/tmp}
plan_file=${HATRIE_TMP_AUDIT_PLAN:-"$tmp_root/.hatrie-cache-tmp-audit.plan"}
mode=${1:-audit}

shopt -s nullglob dotglob

if [[ "$mode" == clean-metadata ]]; then
  removed=0
  for metadata in \
    "$tmp_root/.hatrie-cache-tmp-audit.plan" \
    "$tmp_root/hatrie-cache-tmp-audit.plan" \
    "$tmp_root/hatrie-cache-tmp-cleanup.plan" \
    "$PWD/.hatrie-tmp-cleanup.plan"; do
    if [[ -e "$metadata" ]]; then
      rm -f -- "$metadata"
      printf 'Removed cleanup metadata: %s\n' "$metadata"
      removed=$((removed + 1))
    fi
  done
  printf 'Removed %s cleanup metadata file(s).\n' "$removed"
  exit 0
fi

if [[ "$mode" != audit ]]; then
  printf 'usage: %s [audit|clean-metadata]\n' "$0" >&2
  exit 2
fi

is_hatrie_name() {
  local name=$1
  [[ "$name" =~ [Hh][Aa][Tt][Rr][Ii][Ee] ]]
}

is_go_build_name() {
  local name=$1
  [[ "$name" == go-build* ]]
}

is_active_worktree() {
  local path=$1
  [[ -e "$path/.git" ]]
}

is_tool_metadata() {
  local path=$1
  case "${path##*/}" in
    .hatrie-cache-tmp-audit.plan|hatrie-cache-tmp-audit.plan|hatrie-cache-tmp-cleanup.plan)
      return 0
      ;;
  esac
  return 1
}

format_age() {
  local path=$1
  local now mtime
  now=$(date +%s)
  mtime=$(stat -c '%Y' "$path")
  printf '%ss' "$((now - mtime))"
}

format_size() {
  local path=$1
  du -sh -- "$path" 2>/dev/null | cut -f1
}

printf 'Top-level /tmp Hatrie/build inventory: %s\n' "$tmp_root"
printf 'Protected rule: any directory containing .git is never deleted.\n'
printf 'Review-only rule: generic go-build* paths are listed but never deleted by this tool.\n'

: > "$plan_file"
candidate_count=0
review_count=0

for path in "$tmp_root"/*; do
  [[ -e "$path" || -L "$path" ]] || continue
  [[ "$path" == "$plan_file" ]] && continue

  name=${path##*/}
  if is_tool_metadata "$path"; then
    printf '%-20s %-22s path=%s\n' "TOOL-METADATA-SKIP" "HATRIE-CLEANUP" "$path"
    continue
  fi
  if is_hatrie_name "$name"; then
    category=HATRIE
  elif is_go_build_name "$name"; then
    category=GO-BUILD-REVIEW
  else
    continue
  fi

  if [[ -L "$path" ]]; then
    state=SYMLINK-SKIP
  elif is_active_worktree "$path"; then
    state=ACTIVE-WORKTREE-SKIP
  elif [[ "$category" == GO-BUILD-REVIEW ]]; then
    state=REVIEW-ONLY
    review_count=$((review_count + 1))
  elif [[ -d "$path" || -f "$path" ]]; then
    state=PLAN-CANDIDATE
    printf '%s\n' "$path" >> "$plan_file"
    candidate_count=$((candidate_count + 1))
  else
    state=UNSUPPORTED-SKIP
  fi

  printf '%-20s %-22s age=%-10s size=%s path=%s\n' \
    "$state" "$category" "$(format_age "$path")" "$(format_size "$path")" "$path"
done

if (( candidate_count == 0 )); then
  rm -f -- "$plan_file"
  printf 'Plan: none\n'
else
  printf 'Plan: %s candidate(s) in %s\n' "$candidate_count" "$plan_file"
fi
printf 'Review-only generic go-build entries: %s\n' "$review_count"
