#!/usr/bin/env sh
set -eu

data_dir=${DATA_DIR:-data}
backup_dir=${BACKUP_DIR:-backup/latest}
overwrite=${BACKUP_OVERWRITE:-false}

fail() {
	echo "backup: $*" >&2
	exit 1
}

bool_true() {
	case "$1" in
		1|true|TRUE|yes|YES|on|ON)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

has_entries() {
	[ -d "$1" ] || return 1
	[ -n "$(find "$1" -mindepth 1 -maxdepth 1 -print -quit)" ]
}

absolute_existing_dir() {
	[ -d "$1" ] || fail "$1 is not a directory"
	(
		cd "$1"
		pwd -P
	)
}

absolute_target_dir() {
	parent=$(dirname "$1")
	base=$(basename "$1")
	mkdir -p "$parent"
	parent_abs=$(
		cd "$parent"
		pwd -P
	)
	printf '%s/%s\n' "$parent_abs" "$base"
}

[ -n "$data_dir" ] || fail "DATA_DIR is required"
[ -n "$backup_dir" ] || fail "BACKUP_DIR is required"
[ -d "$data_dir" ] || fail "DATA_DIR does not exist: $data_dir"

data_abs=$(absolute_existing_dir "$data_dir")
backup_abs=$(absolute_target_dir "$backup_dir")

case "$backup_abs" in
	"$data_abs"|"$data_abs"/*)
		fail "BACKUP_DIR must not be inside DATA_DIR"
		;;
esac

mkdir -p "$backup_abs"
if has_entries "$backup_abs" && ! bool_true "$overwrite"; then
	fail "BACKUP_DIR is not empty: $backup_dir; set BACKUP_OVERWRITE=true to copy into it"
fi

cp -a "$data_abs/." "$backup_abs/"
echo "backup: copied $data_abs to $backup_abs"
