#!/usr/bin/env bash
set -euo pipefail

root=${TMP_ROOT:-/tmp}
max_depth=${TMP_SCAN_DEPTH:-3}

printf 'Hatrie temporary directory inventory: %s\n' "$root"
printf 'Scan depth: %s\n' "$max_depth"
printf 'Protected rule: any directory containing .git is never deleted.\n'

found=0
while IFS= read -r -d '' path; do
	base=${path##*/}
	case "$base" in
		*hatrie*|*Hatrie*|*hat-trie*|*hat_trie*)
			found=$((found + 1))
			marker=
			cursor=$path
			while [[ "$cursor" == "$root"/* ]]; do
				if [[ -e "$cursor/.git" ]]; then
					marker="$cursor/.git"
					break
				fi
				parent=${cursor%/*}
				if [[ "$parent" == "$cursor" ]]; then
					break
				fi
				cursor=$parent
			done
			protection=deletable
			if [[ -n "$marker" ]]; then
				protection=protected-git
			fi
			bytes=$(du -sk "$path" 2>/dev/null | awk '{print $1 * 1024}')
			modified=$(stat -c '%Y' "$path")
			now=$(date +%s)
			age=$((now - modified))
			printf '%s\t%s\t%s\t%s\t%s\n' "$protection" "$age" "$bytes" "$path" "$marker"
			;;
	esac
done < <(find "$root" -xdev -mindepth 1 -maxdepth "$max_depth" -type d -print0 2>/dev/null | sort -z)

if [[ "$found" == 1 ]]; then
	printf 'Summary: 1 Hatrie-named directory found.\n'
else
	printf 'Summary: %s Hatrie-named directories found.\n' "$found"
fi
