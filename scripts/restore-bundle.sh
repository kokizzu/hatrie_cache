#!/usr/bin/env sh
set -eu

bundle_path=${RESTORE_BUNDLE_PATH:-}
data_dir=${DATA_DIR:-data}
overwrite=${RESTORE_BUNDLE_OVERWRITE:-false}
resume=${RESTORE_BUNDLE_RESUME:-false}
partitions=${RESTORE_BUNDLE_PARTITIONS:-}
partition_prefixes=${RESTORE_BUNDLE_PARTITION_PREFIXES:-}

if [ -z "$bundle_path" ]; then
	echo "restore-bundle: RESTORE_BUNDLE_PATH is required" >&2
	exit 2
fi

set -- restore-bundle -bundle "$bundle_path" -data-dir "$data_dir"
if [ -n "$partitions" ]; then
	set -- "$@" -partitions "$partitions"
fi
if [ -n "$partition_prefixes" ]; then
	set -- "$@" -partition-prefixes "$partition_prefixes"
fi
case "$overwrite" in
	1|true|TRUE|yes|YES|on|ON)
		set -- "$@" -overwrite
		;;
esac
case "$resume" in
	1|true|TRUE|yes|YES|on|ON)
		set -- "$@" -resume
		;;
esac

exec go run ./cmd/hatrie-cli "$@"
