#!/usr/bin/env sh
set -eu

data_dir=${DATA_DIR:-data}
backup_dir=${BACKUP_DIR:-backup/latest}
overwrite=${RESTORE_OVERWRITE:-false}
script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)

fail() {
	echo "restore: $*" >&2
	exit 1
}

[ -n "$data_dir" ] || fail "DATA_DIR is required"
[ -n "$backup_dir" ] || fail "BACKUP_DIR is required"
[ -d "$backup_dir" ] || fail "BACKUP_DIR does not exist: $backup_dir"

"$script_dir/copy-directory-exact.sh" restore "$backup_dir" "$data_dir" "$overwrite"
echo "restore: copied $backup_dir to $data_dir"
