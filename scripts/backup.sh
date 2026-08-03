#!/usr/bin/env sh
set -eu

data_dir=${DATA_DIR:-data}
backup_dir=${BACKUP_DIR:-backup/latest}
overwrite=${BACKUP_OVERWRITE:-false}
script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)

fail() {
	echo "backup: $*" >&2
	exit 1
}

[ -n "$data_dir" ] || fail "DATA_DIR is required"
[ -n "$backup_dir" ] || fail "BACKUP_DIR is required"
[ -d "$data_dir" ] || fail "DATA_DIR does not exist: $data_dir"

"$script_dir/copy-directory-exact.sh" backup "$data_dir" "$backup_dir" "$overwrite"
echo "backup: copied $data_dir to $backup_dir"
