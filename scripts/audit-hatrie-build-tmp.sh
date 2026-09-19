#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Hatrie temporary entries under /tmp:'
shopt -s nullglob
paths=(/tmp/hatrie*)
if (( ${#paths[@]} == 0 )); then
	printf '%s\n' '(none)'
	exit 0
fi
for path in "${paths[@]}"; do
	if [[ -e "$path" ]]; then
		stat -c '%y %F %s bytes %n' -- "$path"
	fi
done
