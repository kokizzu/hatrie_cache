#!/usr/bin/env bash
set -euo pipefail

mode="${1:-audit}"
tmp_root="/tmp"
plan_file="${HATRIE_TMP_CLEANUP_PLAN:-/tmp/.hatrie-tmp-cleanup.plan}"
repo_root="$(pwd -P)"

case "$mode" in
  audit|plan|preview|apply)
    ;;
  *)
  printf 'usage: %s [audit|plan|preview|apply]\n' "$0" >&2
    exit 2
    ;;
esac

if [[ ! -d "$tmp_root" ]]; then
  printf 'tmp root is missing: %s\n' "$tmp_root" >&2
  exit 1
fi

worktree_file="$(mktemp /tmp/.hatrie-worktrees.XXXXXX)"
trap 'rm -f -- "$worktree_file"' EXIT
git -C "$repo_root" worktree list --porcelain > "$worktree_file"

declare -a active_worktrees=()
while IFS= read -r line; do
  case "$line" in
    worktree\ *)
      active_worktrees+=("${line#worktree }")
      ;;
  esac
done < "$worktree_file"

is_active_worktree() {
  local candidate="$1"
  local active
  for active in "${active_worktrees[@]}"; do
    if [[ "$candidate" == "$active" || "$candidate" == "$active"/* ]]; then
      return 0
    fi
  done
  return 1
}

is_worktree_like() {
  local name="$1"
  case "$name" in
    hatrie-cache-inspiration-*|hatrie-cache-worktree-*|hatrie-worktree-*)
      return 0
      ;;
  esac
  return 1
}

is_generated_candidate() {
  local path="$1"
  local name="${path##*/}"
  if is_active_worktree "$path" || is_worktree_like "$name"; then
    return 1
  fi
  case "$name" in
    hatrie-build-*|hatrie-test-*|hatrie-cache-test-*|hatrie_cache_test_*|hatrie-*-gocache.*|hatrie-gocache.*|hatrie-*-test.*|hatrie-*.tmp|hatrie-*.plan|hatrie-*.cover|hatrie-*.prof|hatrie-*.log|hatrie-*.out)
      return 0
      ;;
  esac
  return 1
}

describe_path() {
  local path="$1"
  local name="${path##*/}"
  local kind="REVIEW"
  if is_active_worktree "$path"; then
    kind="KEEP active-worktree"
  elif is_worktree_like "$name"; then
    kind="KEEP worktree-like"
  elif is_generated_candidate "$path"; then
    kind="CANDIDATE generated"
  fi
  printf '%s\t%s\t' "$kind" "$path"
  du -sh -- "$path" 2>/dev/null || printf '?\t'
  stat -c '%y' -- "$path" 2>/dev/null || printf 'unknown\n'
}

collect_entries() {
  local path
  while IFS= read -r -d '' path; do
    describe_path "$path"
  done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 -name 'hatrie*' -print0)
}

write_plan() {
  local path
  : > "$plan_file"
  while IFS= read -r -d '' path; do
    if is_generated_candidate "$path"; then
      printf '%s\n' "$path" >> "$plan_file"
    fi
  done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 -name 'hatrie*' -print0)
}

case "$mode" in
  audit)
    printf 'Hatrie /tmp audit (no changes)\n'
    printf 'Registered worktrees are protected, including nested contents.\n'
    collect_entries
    ;;
  plan|preview)
    write_plan
    printf 'Hatrie /tmp cleanup preview\n'
    printf 'Plan: %s\n' "$plan_file"
    if [[ -s "$plan_file" ]]; then
      while IFS= read -r path; do
        describe_path "$path"
      done < "$plan_file"
    else
      printf 'Plan: none\n'
    fi
    ;;
  apply)
    if [[ ! -f "$plan_file" ]]; then
      printf 'cleanup plan is missing; run cleanup-hatrie-tmp-preview first: %s\n' "$plan_file" >&2
      exit 1
    fi
    removed=0
    while IFS= read -r path; do
      [[ -n "$path" ]] || continue
      if [[ "$path" != "$tmp_root"/* ]]; then
        printf 'refusing path outside /tmp: %s\n' "$path" >&2
        exit 1
      fi
      if [[ ! -e "$path" && ! -L "$path" ]]; then
        printf 'already absent: %s\n' "$path"
        continue
      fi
      if ! is_generated_candidate "$path"; then
        printf 'refusing changed or protected path: %s\n' "$path" >&2
        exit 1
      fi
      rm -rf -- "$path"
      printf 'removed: %s\n' "$path"
      removed=$((removed + 1))
    done < "$plan_file"
    rm -f -- "$plan_file"
    printf 'Removed: %d\n' "$removed"
    ;;
esac
