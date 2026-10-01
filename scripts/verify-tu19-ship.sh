#!/usr/bin/env bash
set -euo pipefail

temporary_paths=(
    scripts/inspect-round28-candidates.sh
    scripts/inspect-round28-gaps.sh
    scripts/inspect-tu19.sh
    scripts/inspect-tu19-targeted.sh
    scripts/inspect-tu19-files.sh
    scripts/inspect-benchmark-tail-round28.sh
    scripts/inspect-make-round28.sh
)

for path in "${temporary_paths[@]}"; do
    if [[ -e "$path" ]]; then
        printf 'temporary path remains: %s\n' "$path" >&2
        exit 1
    fi
done

git diff --check
git status --short --untracked-files=all
