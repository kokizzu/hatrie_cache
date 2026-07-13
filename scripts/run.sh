#!/usr/bin/env sh
set -eu

if [ "$#" -gt 0 ]; then
	exec "$@"
fi

if [ -z "${CMD:-}" ]; then
	echo "usage: make run CMD='command args'" >&2
	exit 2
fi

exec sh -c "$CMD"
