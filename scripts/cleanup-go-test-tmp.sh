#!/usr/bin/env bash
set -euo pipefail

plan_file="/tmp/hatrie-go-test-tmp-cleanup.plan"
min_age_minutes=1440

write_plan() {
	: > "$plan_file"
	while IFS= read -r -d '' path; do
		printf '%s\n' "$path" >> "$plan_file"
	done < <(find /tmp -mindepth 1 -maxdepth 1 -type d -name '.tmp??????' -mmin +"$min_age_minutes" -print0)
}

case "${1:-}" in
preview)
	write_plan
	printf 'Go/test temporary cleanup plan (age > 24 hours, empty directories only at apply):\n'
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
		if [[ "$path" != /tmp/.tmp?????? || ! -d "$path" ]]; then
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
