#!/usr/bin/env bash
set -euo pipefail

tmp_root=${TMPDIR:-/tmp}
min_age_hours=${HATRIE_TMP_MIN_AGE_HOURS:-24}
repo_root=$(pwd -P)
plan_file=${HATRIE_TMP_CLEANUP_PLAN:-"$repo_root/.hatrie-tmp-cleanup.plan"}

is_hatrie_directory() {
  local path=$1
  local name=${path##*/}
  [[ "$name" == hatrie-cache-* || "$name" == hatrie_cache_* ]]
}

is_active_path() {
  local candidate=$1
  local proc_dir cwd
  for proc_dir in /proc/[0-9]*; do
    [[ -d "$proc_dir" ]] || continue
    cwd=$(readlink "$proc_dir/cwd" 2>/dev/null || true)
    [[ -n "$cwd" ]] || continue
    case "$cwd" in
      "$candidate"|"$candidate"/*)
        return 0
        ;;
    esac
  done
  return 1
}

write_plan() {
  local now=$1
  local path mtime age_hours
  local candidates=0

  : > "$plan_file"
  while IFS= read -r -d '' path; do
    is_hatrie_directory "$path" || continue
    [[ "$path" != "$repo_root" && "$path" != "$repo_root"/* ]] || continue
    is_active_path "$path" && continue

    mtime=$(stat -c '%Y' -- "$path")
    age_hours=$(( (now - mtime) / 3600 ))
    (( age_hours >= min_age_hours )) || continue
    printf '%s\n' "$path" >> "$plan_file"
    candidates=$((candidates + 1))
  done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 -type d -print0)

  printf 'Hatrie test temporary cleanup plan (age >= %s hours):\n' "$min_age_hours"
  if (( candidates == 0 )); then
    printf 'Summary: 0 candidate(s); plan removed.\n'
    rm -f -- "$plan_file"
    return 0
  fi

  while IFS= read -r path; do
    mtime=$(stat -c '%Y' -- "$path")
    age_hours=$(( (now - mtime) / 3600 ))
    printf '%s hours %s\n' "$age_hours" "$path"
  done < "$plan_file"
  printf 'Summary: %s candidate(s).\n' "$candidates"
  printf 'Plan: %s\n' "$plan_file"
}

apply_plan() {
  local path
  local removed=0
  local skipped=0

  [[ -f "$plan_file" ]] || {
    printf 'No cleanup plan found at %s; run preview first.\n' "$plan_file" >&2
    return 1
  }

  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    [[ "$path" == "$tmp_root"/* ]] || {
      printf 'Refusing plan path outside %s: %s\n' "$tmp_root" "$path" >&2
      return 1
    }
    [[ "$path" != "$tmp_root"/*/* ]] || {
      printf 'Refusing non-top-level plan path: %s\n' "$path" >&2
      return 1
    }
    is_hatrie_directory "$path" || {
      printf 'Refusing non-Hatrie plan path: %s\n' "$path" >&2
      return 1
    }
    [[ -d "$path" ]] || {
      skipped=$((skipped + 1))
      continue
    }
    is_active_path "$path" && {
      printf 'Refusing active path: %s\n' "$path" >&2
      return 1
    }
    rm -rf -- "$path"
    printf 'Removed %s\n' "$path"
    removed=$((removed + 1))
  done < "$plan_file"

  rm -f -- "$plan_file"
  printf 'Summary: %s removed, %s already absent.\n' "$removed" "$skipped"
}

case "${1:-}" in
  preview|plan)
    write_plan "$(date +%s)"
    ;;
  apply)
    apply_plan
    ;;
  *)
    printf 'usage: %s preview|apply\n' "$0" >&2
    exit 2
    ;;
esac
