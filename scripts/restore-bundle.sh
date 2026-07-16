#!/usr/bin/env sh
set -eu

bundle_path=${RESTORE_BUNDLE_PATH:-}
data_dir=${DATA_DIR:-data}
overwrite=${RESTORE_BUNDLE_OVERWRITE:-false}

if [ -z "$bundle_path" ]; then
	echo "restore-bundle: RESTORE_BUNDLE_PATH is required" >&2
	exit 2
fi

set -- restore-bundle -bundle "$bundle_path" -data-dir "$data_dir"
case "$overwrite" in
	1|true|TRUE|yes|YES|on|ON)
		set -- "$@" -overwrite
		;;
esac

exec go run ./cmd/hatrie-cli "$@"
