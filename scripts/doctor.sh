#!/usr/bin/env sh
set -eu

path=${DOCTOR_PATH:-${BACKUP_DIR:-backup/latest}}

if [ -z "$path" ]; then
	echo "doctor: DOCTOR_PATH is required" >&2
	exit 2
fi

exec go run ./cmd/hatrie-cli doctor -path "$path"
