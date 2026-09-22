#!/usr/bin/env bash
set -euo pipefail

tmp_root=${TMPDIR:-/tmp}
now=$(date +%s)

printf '%s\n' "Hatrie-related temporary inventory: $tmp_root"
printf '%s\n' "Protected: any directory containing .git is never deleted by cleanup targets."
printf '%s\n' ""

found=0
while IFS= read -r -d '' path; do
    found=1
    modified=$(stat -c '%Y' "$path")
    age=$((now - modified))
    size=$(du -sh "$path" 2>/dev/null | cut -f1)
    if [[ -e "$path/.git" ]]; then
        kind=PROTECTED-WORKTREE
    else
        kind=CANDIDATE-REVIEW
    fi
    printf '%-20s age=%-8ss size=%-8s path=%s\n' "$kind" "$age" "$size" "$path"
done < <(
    find "$tmp_root" -maxdepth 1 -mindepth 1 -type d \( \
        -iname '*hatrie*' -o \
        -iname 'go-build*' -o \
        -iname '*test*tmp*' -o \
        -iname '*test*temp*' \
    \) -print0 | sort -z
)

if (( found == 0 )); then
    printf '%s\n' "No matching temporary directories found."
fi
