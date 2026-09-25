#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

feature_files=(
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  scripts/stage-ledger-corrections.sh
  scripts/commit-ledger-corrections.sh
  scripts/push-ledger-corrections.sh
)
for path in "${feature_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    printf 'missing ledger correction file: %s\n' "$path" >&2
    exit 1
  fi
done

temporary_makefile=$(mktemp)
trap 'rm -f "$temporary_makefile"' EXIT
git show HEAD:Makefile > "$temporary_makefile"
cat >> "$temporary_makefile" <<'EOF'

.PHONY: stage-ledger-corrections commit-ledger-corrections push-ledger-corrections
stage-ledger-corrections:
	bash ./scripts/stage-ledger-corrections.sh
commit-ledger-corrections:
	bash ./scripts/commit-ledger-corrections.sh
push-ledger-corrections:
	bash ./scripts/push-ledger-corrections.sh
EOF

git add -- "${feature_files[@]}"
makefile_blob=$(git hash-object -w "$temporary_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
printf 'staged ledger correction files:\n'
git diff --cached --name-only --
