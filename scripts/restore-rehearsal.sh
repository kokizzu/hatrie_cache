#!/usr/bin/env sh
set -eu

path=${RESTORE_REHEARSAL_PATH:-${DOCTOR_PATH:-${BACKUP_DIR:-backup/latest}}}
work_dir=${RESTORE_REHEARSAL_WORK_DIR:-}
keep_work_dir=${RESTORE_REHEARSAL_KEEP_WORK_DIR:-false}
runtime_check=${RESTORE_REHEARSAL_RUNTIME_CHECK:-true}
runtime_get=${RESTORE_REHEARSAL_RUNTIME_GET:-}
runtime_server_bin=${RESTORE_REHEARSAL_RUNTIME_SERVER_BIN:-}

if [ -z "$path" ]; then
	echo "restore-rehearsal: RESTORE_REHEARSAL_PATH is required" >&2
	exit 2
fi

set -- restore-rehearsal -path "$path"
if [ -n "$work_dir" ]; then
	set -- "$@" -work-dir "$work_dir"
fi
set -- "$@" -runtime-check="$runtime_check"
if [ -n "$runtime_server_bin" ]; then
	set -- "$@" -runtime-server-bin "$runtime_server_bin"
fi
case "$keep_work_dir" in
	1|true|TRUE|yes|YES|on|ON)
		set -- "$@" -keep-work-dir
		;;
esac
for item in $runtime_get; do
	set -- "$@" -runtime-get "$item"
done

exec go run ./cmd/hatrie-cli "$@"
