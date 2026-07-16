#!/usr/bin/env sh
set -eu

config_path=${CONFIG_PATH:-}
if [ -n "$config_path" ]; then
	set -- -config "$config_path" "$@"
fi

exec go run ./cmd/hatrie-cache -check-config "$@"
