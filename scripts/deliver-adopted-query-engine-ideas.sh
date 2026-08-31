#!/bin/sh
set -eu

paths='Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md scripts/verify-adopted-query-engine-ideas.sh scripts/inspect-adopted-query-engine-ideas.sh scripts/deliver-adopted-query-engine-ideas.sh'

case "${1:-}" in
commit)
	git diff --check -- $paths
	git add -- $paths
	git commit -m 'document adopted query engine ideas' -- $paths
	;;
push)
	git push
	;;
*)
	echo "usage: $0 {commit|push}" >&2
	exit 2
	;;
esac
