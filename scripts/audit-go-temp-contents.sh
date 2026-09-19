#!/usr/bin/env bash
set -euo pipefail

printf 'Representative top-level Go-style temporary directories:\n'
count=0
while IFS=' ' read -r _ path; do
	[[ -n "$path" ]] || continue
	printf '\n%s\n' "$path"
	find "$path" -maxdepth 2 -mindepth 1 -printf '  %y %p\n'
	count=$((count + 1))
	[[ "$count" -ge 5 ]] && break
done < <(find /tmp -mindepth 1 -maxdepth 1 -type d -name '.tmp??????' -printf '%T@ %p\n' | sort -n)

if [[ "$count" -eq 0 ]]; then
	printf '(none)\n'
fi
