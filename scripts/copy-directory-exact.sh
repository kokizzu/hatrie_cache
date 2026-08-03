#!/usr/bin/env sh
set -eu

operation=${1:-copy-directory}
source_dir=${2:-}
target_dir=${3:-}
overwrite=${4:-false}
staging_dir=
rollback_dir=
target_abs=

case "$operation" in
	backup)
		source_name=DATA_DIR
		target_name=BACKUP_DIR
		overwrite_name=BACKUP_OVERWRITE
		;;
	restore)
		source_name=BACKUP_DIR
		target_name=DATA_DIR
		overwrite_name=RESTORE_OVERWRITE
		;;
	*)
		source_name=source
		target_name=target
		overwrite_name=overwrite
		;;
esac

fail() {
	echo "$operation: $*" >&2
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

cleanup() {
	if [ -n "$rollback_dir" ] && [ -d "$rollback_dir" ]; then
		if [ -n "$target_abs" ] && [ ! -e "$target_abs" ]; then
			mv "$rollback_dir" "$target_abs" 2>/dev/null || true
		elif [ -n "$target_abs" ] && [ -e "$target_abs" ]; then
			rm -rf "$rollback_dir"
		fi
		rollback_dir=
	fi
	if [ -n "$staging_dir" ] && [ -d "$staging_dir" ]; then
		rm -rf "$staging_dir"
	fi
}

trap cleanup EXIT
trap 'exit 1' HUP INT TERM

[ -n "$source_dir" ] || fail "$source_name is required"
[ -n "$target_dir" ] || fail "$target_name is required"
[ -d "$source_dir" ] || fail "$source_name does not exist: $source_dir"
[ ! -L "$target_dir" ] || fail "$target_name must not be a symlink: $target_dir"
[ ! -e "$target_dir" ] || [ -d "$target_dir" ] || fail "$target_name is not a directory: $target_dir"

source_abs=$(absolute_existing_dir "$source_dir")
target_abs=$(absolute_target_dir "$target_dir")

case "$target_abs" in
	"$source_abs"|"$source_abs"/*)
		fail "$target_name must not be inside $source_name"
		;;
esac
case "$source_abs" in
	"$target_abs"/*)
		fail "$source_name must not be inside $target_name"
		;;
esac

if has_entries "$target_abs" && ! bool_true "$overwrite"; then
	fail "$target_name is not empty: $target_dir; set $overwrite_name=true to replace it"
fi

target_parent=$(dirname "$target_abs")
target_base=$(basename "$target_abs")
staging_dir=$(mktemp -d "$target_parent/.${target_base}.copy.XXXXXX")
cp -a "$source_abs/." "$staging_dir/"

if [ -e "$target_abs" ]; then
	rollback_dir=$(mktemp -d "$target_parent/.${target_base}.rollback.XXXXXX")
	rmdir "$rollback_dir"
	mv "$target_abs" "$rollback_dir"
fi

if ! mv "$staging_dir" "$target_abs"; then
	if [ -n "$rollback_dir" ] && [ -d "$rollback_dir" ]; then
		mv "$rollback_dir" "$target_abs" || true
		rollback_dir=
	fi
	fail "could not publish copied directory"
fi
staging_dir=

if [ -n "$rollback_dir" ]; then
	rm -rf "$rollback_dir"
	rollback_dir=
fi

trap - EXIT HUP INT TERM
