#!/usr/bin/env bash
set -euo pipefail

mode=${1:?expected commit or push}
case "$mode" in
commit)
	git diff --cached --check
	if git diff --cached --quiet --; then
		printf '%s\n' 'no staged CH-031 changes to commit' >&2
		exit 1
	fi
	git commit -m '[skip ci] Add automatic typed JSON subcolumns'
	;;
push)
	git push origin HEAD:master
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
