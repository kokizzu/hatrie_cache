#!/usr/bin/env bash
set -euo pipefail

tmp_root=${TMPDIR:-/tmp}
now=$(date +%s)
seen=0

printf 'Top-level Hatrie-related temporary inventory: %s\n' "$tmp_root"
printf 'Protected rule: entries containing .git are never cleanup candidates.\n'
printf 'Review-only rule: generic go-build* entries are listed but never deleted by Hatrie cleanup.\n'

while IFS= read -r -d '' path; do
    name=${path##*/}
    if [[ ! "$name" =~ [Hh][Aa][Tt][Rr][Ii][Ee] && ! "$name" =~ ^go-build ]]; then
        continue
    fi

    seen=$((seen + 1))
    mtime=$(stat -c %Y -- "$path")
    age=$((now - mtime))
    size=$(du -sh -- "$path" 2>/dev/null | cut -f1)
    if [[ -e "$path/.git" ]]; then
        printf 'PROTECTED-WORKTREE %-28s age=%-8ss size=%-8s path=%s\n' "$name" "$age" "$size" "$path"
    elif [[ "$name" == go-build* ]]; then
        printf 'REVIEW-ONLY-GO-BUILD %-23s age=%-8ss size=%-8s path=%s\n' "$name" "$age" "$size" "$path"
    else
        printf 'CLEANUP-CANDIDATE %-26s age=%-8ss size=%-8s path=%s\n' "$name" "$age" "$size" "$path"
    fi
done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 -print0 | sort -z)

if (( seen == 1 )); then
    printf 'Summary: 1 matching top-level entry.\n'
else
    printf 'Summary: %s matching top-level entries.\n' "$seen"
fi
