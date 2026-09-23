#!/usr/bin/env bash
set -euo pipefail

mode="${1:-plan}"
plan_path="${HATRIE_GO_BUILD_CLEANUP_PLAN:-.hatrie-go-build-cleanup.plan}"
min_age="${HATRIE_GO_BUILD_MIN_AGE_SECONDS:-300}"

if ! [[ "${min_age}" =~ ^[0-9]+$ ]]; then
  printf 'invalid HATRIE_GO_BUILD_MIN_AGE_SECONDS: %s\n' "${min_age}" >&2
  exit 1
fi

case "${mode}" in
plan)
	: > "${plan_path}"
	now="$(date +%s)"
	printf 'Generic Go build temporary cleanup plan (age >= %ss):\n' "${min_age}"
	for path in /tmp/go-build*; do
		[[ -d "${path}" ]] || continue
		base="${path##*/}"
		[[ "${base}" =~ ^go-build[0-9]+$ ]] || continue
		modified="$(stat -c %Y "${path}")"
		age=$((now - modified))
		if (( age < min_age )); then
			printf 'RECENT-SKIP age=%ss path=%s\n' "${age}" "${path}"
			continue
		fi
		printf '%s\t%s\t%s\n' "${path}" "${modified}" "${age}" >> "${plan_path}"
		printf 'CANDIDATE age=%ss path=%s\n' "${age}" "${path}"
	done
	count="$(wc -l < "${plan_path}")"
	printf 'Summary: %s candidate(s). Plan: %s\n' "${count}" "${plan_path}"
	;;
apply)
	if [[ ! -f "${plan_path}" ]]; then
		printf 'cleanup plan does not exist: %s\n' "${plan_path}" >&2
		exit 1
	fi
	removed=0
	while IFS=$'\t' read -r path expected_modified expected_age; do
		[[ -n "${path}" ]] || continue
		base="${path##*/}"
		suffix="${path#/tmp/go-build}"
		if [[ "${path}" != /tmp/go-build* || ! "${base}" =~ ^go-build[0-9]+$ || ! "${suffix}" =~ ^[0-9]+$ ]]; then
			printf 'unsafe cleanup plan path: %s\n' "${path}" >&2
			exit 1
		fi
		if [[ ! -d "${path}" ]]; then
			printf 'already absent: %s\n' "${path}"
			continue
		fi
		actual_modified="$(stat -c %Y "${path}")"
		if [[ "${actual_modified}" != "${expected_modified}" ]]; then
			printf 'modified since plan; refusing: %s\n' "${path}" >&2
			exit 1
		fi
		rm -rf -- "${path}"
		removed=$((removed + 1))
		printf 'removed: %s\n' "${path}"
	done < "${plan_path}"
	printf 'Removed %s candidate(s).\n' "${removed}"
	;;
clean-metadata)
	rm -f -- "${plan_path}"
	printf 'Removed cleanup metadata: %s\n' "${plan_path}"
	;;
*)
	printf 'usage: %s {plan|apply|clean-metadata}\n' "$0" >&2
	exit 1
	;;
esac
