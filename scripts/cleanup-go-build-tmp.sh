#!/usr/bin/env bash
set -euo pipefail

mode="${1:-plan}"
plan_file=".hatrie-go-build-cleanup.plan"

case "$mode" in
plan)
	: > "$plan_file"
	found=0
	for path in /tmp/go-build*; do
		if [[ -d "$path" && ! -L "$path" ]]; then
			printf '%s\n' "$path" >> "$plan_file"
			printf 'CANDIDATE %s\n' "$path"
			found=1
		fi
	done
	if [[ "$found" -eq 0 ]]; then
		rm -f "$plan_file"
		printf '%s\n' 'Plan: none'
	else
		printf 'Plan: %s\n' "$plan_file"
	fi
	;;
apply)
	if [[ ! -f "$plan_file" ]]; then
		printf 'missing cleanup plan: %s\n' "$plan_file" >&2
		exit 1
	fi
	while IFS= read -r path; do
		case "$path" in
			/tmp/go-build*)
				if [[ -d "$path" && ! -L "$path" ]]; then
					rm -rf -- "$path"
					printf 'REMOVED %s\n' "$path"
				else
					printf 'SKIP-MISSING %s\n' "$path"
				fi
				;;
			*)
				printf 'unsafe cleanup plan entry: %s\n' "$path" >&2
				exit 1
				;;
		esac
	done < "$plan_file"
	rm -f "$plan_file"
	printf '%s\n' 'Plan applied and removed.'
	;;
*)
	printf 'usage: %s [plan|apply]\n' "$0" >&2
	exit 2
	;;
esac
