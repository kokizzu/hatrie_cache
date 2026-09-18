#!/usr/bin/env bash
set -euo pipefail

script="$(pwd -P)/scripts/cleanup-hatrie-tmp.sh"
suffix="$$"
root="/tmp/hatrie-cleanup-test-${suffix}-root"
old_dir="$root/hatrie-old"
recent_dir="$root/hatrie-recent"
worktree_dir="$root/hatrie-worktree"
plan="/tmp/hatrie-cleanup-test-${suffix}.plan"

cleanup() {
	rm -rf -- "$root" "$plan"
}
trap cleanup EXIT

mkdir -p "$old_dir" "$recent_dir" "$worktree_dir/.git"
touch -d '2 hours ago' "$old_dir"

HATRIE_TMP_CLEANUP_ROOT="$root" HATRIE_TMP_CLEANUP_PLAN="$plan" HATRIE_TMP_CLEANUP_ACTIVE_WORKTREE="$root/hatrie-active" bash "$script" preview

if ! grep -Fqx "$old_dir" "$plan"; then
	printf 'expected stale directory in cleanup plan: %s\n' "$old_dir" >&2
	exit 1
fi
if grep -Fqx "$recent_dir" "$plan"; then
	printf 'unexpected recent directory in cleanup plan: %s\n' "$recent_dir" >&2
	exit 1
fi
if grep -Fqx "$worktree_dir" "$plan"; then
	printf 'unexpected worktree directory in cleanup plan: %s\n' "$worktree_dir" >&2
	exit 1
fi

HATRIE_TMP_CLEANUP_ROOT="$root" HATRIE_TMP_CLEANUP_PLAN="$plan" HATRIE_TMP_CLEANUP_ACTIVE_WORKTREE="$root/hatrie-active" bash "$script" apply
if [ -e "$old_dir" ]; then
	printf 'stale directory was not removed: %s\n' "$old_dir" >&2
	exit 1
fi
if [ ! -d "$recent_dir" ] || [ ! -d "$worktree_dir/.git" ]; then
	printf '%s\n' 'cleanup removed a protected fixture' >&2
	exit 1
fi

printf '%s\n' 'cleanup-hatrie-tmp contract passed'
