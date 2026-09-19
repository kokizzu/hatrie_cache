#!/usr/bin/env bash
set -euo pipefail

mode=${1:-preview}
plan=/tmp/hatrie-build-tmp-cleanup.plan
max_age=${HATRIE_BUILD_TMP_MAX_AGE_SECONDS:-86400}
if [[ ! "$max_age" =~ ^[0-9]+$ ]]; then
	printf 'invalid HATRIE_BUILD_TMP_MAX_AGE_SECONDS: %s\n' "$max_age" >&2
	exit 1
fi

is_candidate() {
	local base=$1
	case "$base" in
		hatrie-cache-*-benchmark.txt|hatrie-cache-*-sorted-arrangement.mem|hatrie_cache-api-before-restore.*|hatrie_cache-frontiers-before-restore.*|hatrie-origin-*.commit)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

is_protected() {
	local path=$1
	[[ -e "$path/.git" || -L "$path/.git" ]]
}

collect_plan() {
	local now path base mtime age
	local candidates=0 recent=0 protected=0
	: >"$plan"
	shopt -s nullglob
	paths=(/tmp/hatrie*)
	for path in "${paths[@]}"; do
		[[ -e "$path" ]] || continue
		base=${path##*/}
		is_candidate "$base" || continue
		if is_protected "$path"; then
			printf 'protected: %s\n' "$path"
			protected=$((protected + 1))
			continue
		fi
		mtime=$(stat -c %Y -- "$path")
		now=$(date +%s)
		age=$((now - mtime))
		if (( age < max_age )); then
			printf 'recent skip: %s (%ss old)\n' "$path" "$age"
			recent=$((recent + 1))
			continue
		fi
		printf '%s\n' "$path" >>"$plan"
		printf 'candidate: %s (%ss old)\n' "$path" "$age"
		candidates=$((candidates + 1))
	done
	printf 'Summary: %d candidate(s), %d recent skip(s), %d protected skip(s).\n' "$candidates" "$recent" "$protected"
	if (( candidates == 0 )); then
		rm -f "$plan"
	fi
}

case "$mode" in
	preview)
		printf 'Stale Hatrie build/test artifacts under /tmp (age >= %ss):\n' "$max_age"
		collect_plan
		;;
	apply)
		if [[ ! -s "$plan" ]]; then
			printf 'No reviewed cleanup plan at %s; run the preview target first.\n' "$plan" >&2
			exit 1
		fi
		removed=0
		while IFS= read -r path; do
			[[ -n "$path" ]] || continue
			base=${path##*/}
			is_candidate "$base" || {
				printf 'refusing changed plan entry: %s\n' "$path" >&2
				exit 1
			}
			[[ -e "$path" ]] || {
				printf 'refusing missing plan entry: %s\n' "$path" >&2
				exit 1
			}
			is_protected "$path" && {
				printf 'refusing protected plan entry: %s\n' "$path" >&2
				exit 1
			}
			mtime=$(stat -c %Y -- "$path")
			now=$(date +%s)
			(( now - mtime >= max_age )) || {
				printf 'refusing recent plan entry: %s\n' "$path" >&2
				exit 1
			}
			rm -rf -- "$path"
			printf 'removed: %s\n' "$path"
			removed=$((removed + 1))
		done <"$plan"
		rm -f "$plan"
		printf 'Removed %d reviewed artifact(s).\n' "$removed"
		;;
	verify)
		bash "$0" preview
		;;
	*)
		printf 'usage: %s {preview|apply|verify}\n' "$0" >&2
		exit 2
		;;
esac
