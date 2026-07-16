#!/usr/bin/env sh
set -eu

path=${RESTORE_REHEARSAL_PATH:-${DOCTOR_PATH:-${BACKUP_DIR:-backup/latest}}}
work_dir=${RESTORE_REHEARSAL_WORK_DIR:-}
keep_work_dir=${RESTORE_REHEARSAL_KEEP_WORK_DIR:-false}

if [ -z "$path" ]; then
	echo "restore-rehearsal: RESTORE_REHEARSAL_PATH is required" >&2
	exit 2
fi

set -- restore-rehearsal -path "$path"
if [ -n "$work_dir" ]; then
	set -- "$@" -work-dir "$work_dir"
fi
case "$keep_work_dir" in
	1|true|TRUE|yes|YES|on|ON)
		set -- "$@" -keep-work-dir
		;;
esac

exec go run ./cmd/hatrie-cli "$@"
