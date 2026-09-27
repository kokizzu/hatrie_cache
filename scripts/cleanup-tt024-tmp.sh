#!/usr/bin/env bash
set -euo pipefail

plan_file="${TMPDIR:-/tmp}/hatrie-tt024-cleanup.plan"
mode="${1:-preview}"

case "$mode" in
preview)
	find "${TMPDIR:-/tmp}" -mindepth 1 -maxdepth 1 -type d -name 'hatrie-tt024-test.*' -print | sort >"$plan_file"
	printf '%s\n' 'TT-024 temporary cleanup plan:'
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
		printf '%s\n' 'No reviewed TT-024 cleanup plan exists.' >&2
		exit 1
	fi
	count=0
	while IFS= read -r path; do
		[ -n "$path" ] || continue
		case "$path" in
			"${TMPDIR:-/tmp}"/hatrie-tt024-test.*)
				[ -d "$path" ] || continue
				chmod -R u+w -- "$path" 2>/dev/null || true
				rm -rf -- "$path"
				count=$((count + 1))
				;;
			*)
				printf 'Refusing unexpected cleanup path: %s\n' "$path" >&2
				exit 1
				;;
		esac
	done <"$plan_file"
	rm -f -- "$plan_file"
	printf 'Removed %d TT-024 temporary directory/directories.\n' "$count"
	;;
*)
	printf 'usage: %s [preview|apply]\n' "$0" >&2
	exit 2
;;
esac
