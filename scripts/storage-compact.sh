#!/usr/bin/env sh
set -eu

peer=${STORAGE_PEER:-http://127.0.0.1:8080}
start_key=${STORAGE_COMPACT_START_KEY:-}
limit_key=${STORAGE_COMPACT_LIMIT_KEY:-}

set -- -addr "$peer" storage compact
if [ -n "$start_key" ]; then
	set -- "$@" -start-key "$start_key"
fi
if [ -n "$limit_key" ]; then
	set -- "$@" -limit-key "$limit_key"
fi

exec ./scripts/cli.sh "$@"
