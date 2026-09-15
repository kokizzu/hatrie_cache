#!/usr/bin/env bash
set -euo pipefail

script="$(pwd -P)/scripts/cleanup-test-tmp.sh"
root="$(mktemp -d)"
plan="$root/cleanup.plan"
trap 'rm -rf -- "$root"' EXIT

old_candidate="$root/TestOldGoTemp"
recent_candidate="$root/TestRecentGoTemp"
protected_candidate="$root/hatrie-cache-c203"
unrelated_directory="$root/go-build-cache"

mkdir -p "$old_candidate" "$recent_candidate" "$protected_candidate" "$unrelated_directory"
printf 'worktree\n' > "$protected_candidate/.git"
touch -d '2 days ago' "$old_candidate"
touch -d '2 days ago' "$unrelated_directory"

TMP_CLEANUP_ROOT="$root" TMP_CLEANUP_PLAN="$plan" bash "$script" preview > "$root/preview.txt"
grep -F "$old_candidate" "$plan"
if grep -F "$recent_candidate" "$plan"; then
  printf 'recent candidate was incorrectly selected\n' >&2
  exit 1
fi
if grep -F "$protected_candidate" "$plan"; then
  printf 'protected worktree was incorrectly selected\n' >&2
  exit 1
fi
if grep -F "$unrelated_directory" "$plan"; then
  printf 'unrelated directory was incorrectly selected\n' >&2
  exit 1
fi

TMP_CLEANUP_ROOT="$root" TMP_CLEANUP_PLAN="$plan" bash "$script" apply > "$root/apply.txt"
[[ ! -e "$old_candidate" ]]
[[ -d "$recent_candidate" ]]
[[ -d "$protected_candidate" ]]
[[ -d "$unrelated_directory" ]]
[[ ! -e "$plan" ]]

TMP_CLEANUP_ROOT="$root" TMP_CLEANUP_PLAN="$plan" bash "$script" preview > "$root/preview-empty.txt"
[[ ! -e "$plan" ]]

mkdir "$old_candidate"
touch -d '2 days ago' "$old_candidate"
TMP_CLEANUP_ROOT="$root" TMP_CLEANUP_PLAN="$plan" bash "$script" preview > "$root/preview-recheck.txt"
touch "$old_candidate"
if TMP_CLEANUP_ROOT="$root" TMP_CLEANUP_PLAN="$plan" bash "$script" apply > "$root/apply-recheck.txt" 2>&1; then
  printf 'apply unexpectedly removed a directory that became recent\n' >&2
  exit 1
fi
[[ -d "$old_candidate" ]]

printf 'cleanup-test-tmp contract passed\n'
