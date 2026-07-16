#!/usr/bin/env sh
set -eu

peer=${STORAGE_PEER:-http://127.0.0.1:8080}

exec ./scripts/cli.sh -addr "$peer" storage flush
