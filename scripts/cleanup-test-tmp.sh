#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
case "$mode" in
preview|apply)
  ;;
*)
  printf 'usage: %s [preview|apply]\n' "$0" >&2
  exit 2
  ;;
esac

tmp_root="${TMP_CLEANUP_ROOT:-/tmp}"
max_age_hours="${TMP_CLEANUP_MAX_AGE_HOURS:-24}"
plan_path="${TMP_CLEANUP_PLAN:-/tmp/hatrie-cache-test-tmp.plan}"
repo_root="$(pwd -P)"

case "$tmp_root" in
/*)
  ;;
*)
  printf 'TMP_CLEANUP_ROOT must be an absolute path: %s\n' "$tmp_root" >&2
  exit 2
  ;;
esac
if [[ "$tmp_root" == "/" ]]; then
  printf 'TMP_CLEANUP_ROOT may not be /\n' >&2
  exit 2
fi
if [[ ! "$max_age_hours" =~ ^[0-9]+$ ]] || (( max_age_hours < 1 )); then
  printf 'TMP_CLEANUP_MAX_AGE_HOURS must be a positive integer: %s\n' "$max_age_hours" >&2
  exit 2
fi
if [[ ! -d "$tmp_root" ]]; then
  printf 'temporary root does not exist: %s\n' "$tmp_root" >&2
  exit 2
fi

now="$(date +%s)"
cutoff=$((now - max_age_hours * 60 * 60))

is_candidate_name() {
  case "$(basename "$1")" in
  Test*|hatrie-cache-restore-rehearsal-*|hatrie-cache-restore-*)
    return 0
    ;;
  *)
    return 1
    ;;
  esac
}

is_protected() {
  local path="$1"
  if [[ "$path" == "$repo_root" || "$path" == "$repo_root"/* ]]; then
    return 0
  fi
  if [[ -e "$path/.git" ]]; then
    return 0
  fi
  return 1
}

is_old_enough() {
  local path="$1"
  [[ "$(stat -c %Y -- "$path")" -le "$cutoff" ]]
}

describe_path() {
  local path="$1"
  local mtime
  local size
  mtime="$(stat -c %Y -- "$path")"
  size="$(du -sh -- "$path" | cut -f1)"
  printf '%s\t%s\t%s\n' "$(date -d "@$mtime" --iso-8601=seconds)" "$size" "$path"
}

if [[ "$mode" == "preview" ]]; then
  : > "$plan_path"
  candidates=0
  skipped_recent=0
  skipped_protected=0

  printf 'Stale test temporary directories under %s (age >= %s hours):\n' "$tmp_root" "$max_age_hours"
  while IFS= read -r -d '' path; do
    is_candidate_name "$path" || continue
    if is_protected "$path"; then
      skipped_protected=$((skipped_protected + 1))
      continue
    fi
    if ! is_old_enough "$path"; then
      skipped_recent=$((skipped_recent + 1))
      continue
    fi
    describe_path "$path" | tee -a "$plan_path"
    candidates=$((candidates + 1))
  done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 -type d -print0)

  printf 'Summary: %s candidate(s), %s recent skip(s), %s protected skip(s).\n' "$candidates" "$skipped_recent" "$skipped_protected"
  printf 'Plan: %s\n' "$plan_path"
  printf 'Review the plan before running cleanup-test-tmp-apply.\n'
  exit 0
fi

if [[ ! -s "$plan_path" ]]; then
  printf 'cleanup plan is empty or missing: %s\n' "$plan_path" >&2
  printf 'Run cleanup-test-tmp-preview first.\n' >&2
  exit 2
fi

removed=0
while IFS=$'\t' read -r _mtime _size path; do
  [[ -n "$path" ]] || continue
  case "$path" in
  "$tmp_root"/*)
    ;;
  *)
    printf 'refusing path outside temporary root: %s\n' "$path" >&2
    exit 2
    ;;
  esac
  [[ -d "$path" ]] || {
    printf 'refusing missing or non-directory path: %s\n' "$path" >&2
    exit 2
  }
  is_candidate_name "$path" || {
    printf 'refusing path with unexpected name: %s\n' "$path" >&2
    exit 2
  }
  is_protected "$path" && {
    printf 'refusing protected path: %s\n' "$path" >&2
    exit 2
  }
  is_old_enough "$path" || {
    printf 'refusing path that became recent: %s\n' "$path" >&2
    exit 2
  }
  printf 'Removing %s\n' "$path"
  rm -rf -- "$path"
  removed=$((removed + 1))
done < "$plan_path"

rm -f -- "$plan_path"
printf 'Removed %s directory/directories.\n' "$removed"
