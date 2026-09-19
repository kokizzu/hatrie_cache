#!/bin/sh
set -eu

mode=${1:-plan}
plan=/tmp/hatrie-cache-tmp-cleanup.current.plan

is_protected() {
	base=${1##*/}
	case "$base" in
		hatrie-cache-next-goal|*next-goal*|*stage*|*session*|hatrie-cache-tmp-cleanup.current.plan)
			return 0
			;;
	esac
	return 1
}

case "$mode" in
	plan)
		: > "$plan"
		for candidate in /tmp/hatrie*; do
			if [ ! -e "$candidate" ] && [ ! -L "$candidate" ]; then
				continue
			fi
			if is_protected "$candidate"; then
				continue
			fi
			printf '%s\n' "$candidate" >> "$plan"
		done
		printf 'review plan: %s\n' "$plan"
		if [ -s "$plan" ]; then
			while IFS= read -r candidate; do
				printf 'REMOVE %s\n' "$candidate"
			done < "$plan"
		else
			printf 'REMOVE none\n'
		fi
		printf 'PRESERVE /tmp/hatrie-cache-next-goal and paths containing next-goal, stage, or session\n'
		;;
	apply)
		if [ ! -f "$plan" ]; then
			printf 'cleanup plan is missing: %s\n' "$plan" >&2
			exit 1
		fi
		while IFS= read -r candidate; do
			[ -n "$candidate" ] || continue
			case "$candidate" in
				/tmp/hatrie*|/tmp/hatri*)
					;;
				*)
					printf 'unexpected cleanup path: %s\n' "$candidate" >&2
					exit 1
					;;
			esac
			if is_protected "$candidate"; then
				printf 'PRESERVE %s\n' "$candidate"
				continue
			fi
			if [ ! -e "$candidate" ] && [ ! -L "$candidate" ]; then
				printf 'GONE %s\n' "$candidate"
				continue
			fi
			printf 'REMOVE %s\n' "$candidate"
			rm -rf -- "$candidate"
		done < "$plan"
		rm -f -- "$plan"
		printf 'cleanup complete: %s\n' "$plan"
		;;
	*)
		printf 'usage: %s {plan|apply}\n' "$0" >&2
		exit 2
		;;
esac
