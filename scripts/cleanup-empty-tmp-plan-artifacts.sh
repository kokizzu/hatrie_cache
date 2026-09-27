#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
plan_file="${TMPDIR:-/tmp}/hatrie-empty-plan-artifacts.review"
paths=(
	"${TMPDIR:-/tmp}/hatrie-cache-test-tmp.plan"
	"${TMPDIR:-/tmp}/hatrie-tt024-cleanup.plan"
)

case "$mode" in
preview)
	: >"$plan_file"
	for path in "${paths[@]}"; do
		if [ -f "$path" ] && [ ! -s "$path" ]; then
			printf '%s\n' "$path" >>"$plan_file"
		fi
	done
	printf '%s\n' 'Empty Hatrie temporary plan cleanup:'
	if [ -s "$plan_file" ]; then
		while IFS= read -r path; do
			printf 'candidate\t%s\n' "$path"
		done <"$plan_file"
	else
		printf '%s\n' 'candidate\t<none>'
	fi
	;;
apply)
	if [ ! -f "$plan_file" ]; then
		printf '%s\n' 'No reviewed empty-plan cleanup exists.' >&2
		exit 1
	fi
	count=0
	while IFS= read -r path; do
		[ -n "$path" ] || continue
		case "$path" in
			"${TMPDIR:-/tmp}"/hatrie-cache-test-tmp.plan|"${TMPDIR:-/tmp}"/hatrie-tt024-cleanup.plan)
				[ -f "$path" ] || continue
				[ ! -s "$path" ] || { printf 'Refusing non-empty plan: %s\n' "$path" >&2; exit 1; }
				rm -f -- "$path"
				count=$((count + 1))
				;;
			*)
				printf 'Refusing unexpected cleanup path: %s\n' "$path" >&2
				exit 1
				;;
		esac
	done <"$plan_file"
	rm -f -- "$plan_file"
	printf 'Removed %d empty Hatrie temporary plan file(s).\n' "$count"
	;;
*)
	printf 'usage: %s [preview|apply]\n' "$0" >&2
	exit 2
;;
esac
