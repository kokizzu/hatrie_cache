#!/usr/bin/env bash
set -euo pipefail

protected_path="/tmp/hatrie-cache-next-goal"
printf 'Hatrie temporary directory audit\n'
printf 'Protected path: %s\n' "$protected_path"
printf 'Matching top-level directories:\n'

found=0
while IFS= read -r -d '' path; do
	found=1
	if [[ "$path" == "$protected_path" ]]; then
		status="PROTECTED"
	else
		status="review"
	fi
	mtime=$(stat -c '%y' "$path")
	size=$(du -sh "$path" | cut -f1)
	printf '%s\t%s\t%s\t%s\n' "$status" "$size" "$mtime" "$path"
done < <(find /tmp -mindepth 1 -maxdepth 1 -type d -iname '*hatrie*' -print0)

if [[ "$found" -eq 0 ]]; then
	printf '(none)\n'
fi

printf 'Nested Hatrie-named paths under /tmp (for build/test leftovers):\n'
found=0
while IFS= read -r -d '' path; do
	found=1
	printf 'review\t%s\n' "$path"
done < <(find /tmp -mindepth 2 -maxdepth 3 \
	-path "$protected_path/*" -prune -o -iname '*hatrie*' -print0 2>/dev/null)

if [[ "$found" -eq 0 ]]; then
	printf '(none)\n'
fi

printf 'Common Go/test temporary directories at /tmp:\n'
found=0
while IFS= read -r -d '' path; do
	if [[ "$path" == "$protected_path" ]]; then
		continue
	fi
	found=1
	mtime=$(stat -c '%y' "$path")
	size=$(du -sh "$path" | cut -f1)
	printf 'review\t%s\t%s\t%s\n' "$size" "$mtime" "$path"
done < <(find /tmp -mindepth 1 -maxdepth 1 -type d \
	\( -name 'go-build*' -o -name 'Test*' -o -iname '*hatrie*' -o -iname '*hat*' -o -name 'tmp*' -o -name '.tmp*' \) -print0)

if [[ "$found" -eq 0 ]]; then
	printf '(none)\n'
fi
