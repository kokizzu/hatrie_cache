#!/usr/bin/env bash
set -euo pipefail

expected_paths=(
  Makefile
  scripts/audit-hatrie-tmp.sh
  scripts/stage-hatrie-tmp-metadata-plan-fix.sh
  scripts/commit-hatrie-tmp-metadata-plan-fix.sh
  scripts/push-hatrie-tmp-metadata-plan-fix.sh
)

mapfile -t existing_staged < <(git diff --cached --name-only --)
if (( ${#existing_staged[@]} != 0 )); then
  printf 'refusing to mix with existing staged paths:\n' >&2
  printf '  %s\n' "${existing_staged[@]}" >&2
  exit 1
fi

for path in "${expected_paths[@]}"; do
  [[ -f "$path" ]] || {
    printf 'required file is missing: %s\n' "$path" >&2
    exit 1
  }
done

tmp_makefile=$(mktemp)
cleanup() {
  rm -f -- "$tmp_makefile"
}
trap cleanup EXIT

git show HEAD:Makefile > "$tmp_makefile"
cat >> "$tmp_makefile" <<'EOF'

.PHONY: stage-hatrie-tmp-metadata-plan-fix commit-hatrie-tmp-metadata-plan-fix push-hatrie-tmp-metadata-plan-fix
stage-hatrie-tmp-metadata-plan-fix:
	bash ./scripts/stage-hatrie-tmp-metadata-plan-fix.sh
commit-hatrie-tmp-metadata-plan-fix:
	bash ./scripts/commit-hatrie-tmp-metadata-plan-fix.sh
push-hatrie-tmp-metadata-plan-fix:
	bash ./scripts/push-hatrie-tmp-metadata-plan-fix.sh
EOF

makefile_blob=$(git hash-object -w "$tmp_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git add -- \
  scripts/audit-hatrie-tmp.sh \
  scripts/stage-hatrie-tmp-metadata-plan-fix.sh \
  scripts/commit-hatrie-tmp-metadata-plan-fix.sh \
  scripts/push-hatrie-tmp-metadata-plan-fix.sh
git diff --cached --check
printf 'Staged Hatrie tmp metadata plan fix paths:\n'
git diff --cached --name-only --
