#!/usr/bin/env sh
set -eu

profile=${CONFIG_PROFILE:-production}

exec go run ./cmd/hatrie-cache -print-config -profile "$profile" "$@"
