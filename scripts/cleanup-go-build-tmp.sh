#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
if [[ "$mode" != "preview" && "$mode" != "apply" ]]; then
	echo "usage: $0 preview|apply" >&2
	exit 2
fi

root="${HATRIE_GO_BUILD_TMP_ROOT:-/tmp}"
plan="${HATRIE_GO_BUILD_CLEANUP_PLAN:-$root/hatrie-go-build-cleanup.plan}"
min_age="${HATRIE_GO_BUILD_MIN_AGE_SECONDS:-300}"
if [[ ! "$min_age" =~ ^[0-9]+$ ]]; then
	echo "HATRIE_GO_BUILD_MIN_AGE_SECONDS must be a non-negative integer" >&2
	exit 2
fi

processes_for_path() {
	local path=$1
	local proc cwd fd target comm
	for proc in /proc/[0-9]*; do
		[[ -d "$proc" ]] || continue
		comm=$(<"$proc/comm") || comm="?"
		cwd=$(readlink "$proc/cwd" 2>/dev/null || true)
		if [[ "$cwd" == "$path" || "$cwd" == "$path/"* ]]; then
			printf '%s(%s) cwd=%s\n' "${proc##*/}" "$comm" "$cwd"
			continue
		fi
		for fd in "$proc"/fd/*; do
			target=$(readlink "$fd" 2>/dev/null || true)
			if [[ "$target" == "$path" || "$target" == "$path/"* ]]; then
				printf '%s(%s) fd=%s target=%s\n' "${proc##*/}" "$comm" "${fd##*/}" "$target"
				break
			fi
		done
	done
}

signature_for_path() {
	stat -c '%d:%i:%s:%Y' -- "$1"
}

size_for_path() {
	du -sh -- "$1" | awk '{print $1}'
}

if [[ "$mode" == "preview" ]]; then
	tmp_plan="$(mktemp /tmp/hatrie-go-build-cleanup.XXXXXX)"
	cleanup_preview() {
		rm -f "$tmp_plan"
	}
	trap cleanup_preview EXIT
	: > "$tmp_plan"
	now=$(date +%s)
	candidates=0
	protected=0
	shopt -s nullglob
	for path in "$root"/go-build*; do
		[[ -d "$path" ]] || continue
		mtime=$(stat -c %Y -- "$path")
		age=$((now - mtime))
		(( age < 0 )) && age=0
		size=$(size_for_path "$path")
		active=$(processes_for_path "$path")
		if [[ -n "$active" ]]; then
			printf 'PROTECTED active age=%ss size=%s path=%s\n%s\n' "$age" "$size" "$path" "$active"
			((protected += 1))
			continue
		fi
		if (( age < min_age )); then
			printf 'RECENT skip age=%ss size=%s path=%s\n' "$age" "$size" "$path"
			((protected += 1))
			continue
		fi
		printf '%s\t%s\n' "$path" "$(signature_for_path "$path")" >> "$tmp_plan"
		printf 'CANDIDATE age=%ss size=%s path=%s\n' "$age" "$size" "$path"
		((candidates += 1))
		done
	shopt -u nullglob
	plan_display="$plan"
	if (( candidates == 0 )); then
		rm -f -- "$tmp_plan" "$plan"
		plan_display="none"
	else
		mv -f -- "$tmp_plan" "$plan"
	fi
	trap - EXIT
	printf 'Summary: %d candidate(s), %d protected/recent skip(s). Plan: %s\n' "$candidates" "$protected" "$plan_display"
	exit 0
fi

if [[ ! -s "$plan" ]]; then
	echo "Plan is missing or empty: $plan" >&2
	exit 1
fi

removed=0
while IFS=$'\t' read -r path expected_signature; do
	[[ -n "$path" ]] || continue
	case "$path" in
		"$root"/go-build*) ;;
		*) echo "refusing out-of-scope path: $path" >&2; exit 1 ;;
	esac
	[[ -d "$path" ]] || { echo "planned path disappeared: $path"; continue; }
	actual_signature=$(signature_for_path "$path")
	if [[ "$actual_signature" != "$expected_signature" ]]; then
		echo "refusing changed path: $path" >&2
		exit 1
	fi
	active=$(processes_for_path "$path")
	if [[ -n "$active" ]]; then
		echo "refusing active path: $path" >&2
	printf '%s\n' "$active" >&2
		exit 1
	fi
	echo "REMOVE $path"
	rm -rf -- "$path"
	((removed += 1))
done < "$plan"
rm -f -- "$plan"
printf 'Removed %d reviewed go-build directory(ies).\n' "$removed"
