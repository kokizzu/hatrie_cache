#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
plan="${HATRIE_TMP_CLEANUP_PLAN:-/tmp/hatrie-cache-tmp-cleanup.plan}"
root="${HATRIE_TMP_CLEANUP_ROOT:-/tmp}"
root="${root%/}"
active_worktree="${HATRIE_TMP_CLEANUP_ACTIVE_WORKTREE:-/tmp/hatrie-cache-next-goal}"

case "$mode" in
preview)
	: > "$plan"
	while IFS= read -r path; do
		case "$path" in
			"$active_worktree"|"$active_worktree"/)
				continue
				;;
		esac
		if [ -e "$path/.git" ]; then
			continue
		fi
		printf '%s\n' "$path" >> "$plan"
	done < <(find "$root" -mindepth 1 -maxdepth 1 -type d -mmin +60 \( -name 'hatrie-*' -o -name 'hatrie_*' \) -print | sort)
	printf '%s\n' 'Cleanup plan (directories older than 60 minutes, no .git marker):'
	if [ -s "$plan" ]; then
		while IFS= read -r path; do
			printf '%s ' "$path"
			du -sh -- "$path" | cut -f1
		done < "$plan"
	else
		printf '%s\n' '(none)'
	fi
	printf 'Plan file: %s\n' "$plan"
	;;
apply)
	if [ ! -e "$plan" ]; then
		printf 'No reviewed cleanup plan found at %s\n' "$plan" >&2
		exit 1
	fi
	if [ ! -s "$plan" ]; then
		rm -f -- "$plan"
		printf '%s\n' 'Cleanup plan is empty; nothing to remove.'
		exit 0
	fi
	while IFS= read -r path; do
		case "$path" in
			"$root"/hatrie-*|"$root"/hatrie_*)
				;;
			*)
				printf 'Refusing unexpected cleanup path: %s\n' "$path" >&2
				exit 1
				;;
		esac
		case "$path" in
			"$active_worktree"|"$active_worktree"/)
				printf 'Refusing active worktree cleanup: %s\n' "$path" >&2
				exit 1
				;;
		esac
		if [ -d "$path" ] && [ ! -e "$path/.git" ]; then
			rm -rf -- "$path"
			printf 'Removed %s\n' "$path"
		else
			printf 'Skipped changed or missing path %s\n' "$path"
		fi
	done < "$plan"
	rm -f -- "$plan"
	;;
*)
	printf 'usage: %s preview|apply\n' "$0" >&2
	exit 2
	;;
esac
