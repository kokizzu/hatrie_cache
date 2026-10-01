#!/usr/bin/env bash
set -euo pipefail

temporary_paths=(
    hat/hatSql/tu05_round_compat.go
    scripts/inspect-round29-catalog.sh
    scripts/inspect-round29-stored-procedure.sh
    scripts/inspect-round29-function-files.sh
    scripts/inspect-round29-function-apis.sh
    scripts/inspect-round29-session-settings.sh
    scripts/inspect-round29-query-options.sh
    scripts/inspect-round29-query-options-api.sh
    scripts/inspect-round29-typed-kinds.sh
    scripts/inspect-round29-doc-placement.sh
)

for path in "${temporary_paths[@]}"; do
    if [[ -e "$path" ]]; then
        printf 'temporary path remains: %s\n' "$path" >&2
        exit 1
    fi
done

git diff --check
git status --short --untracked-files=all
