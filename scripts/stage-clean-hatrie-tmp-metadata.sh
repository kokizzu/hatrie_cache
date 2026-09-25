#!/usr/bin/env bash
set -euo pipefail

expected_paths=(
  Makefile
  scripts/stage-clean-hatrie-tmp-metadata.sh
  scripts/commit-clean-hatrie-tmp-metadata.sh
  scripts/push-clean-hatrie-tmp-metadata.sh
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

.PHONY: clean-hatrie-tmp-metadata stage-clean-hatrie-tmp-metadata commit-clean-hatrie-tmp-metadata push-clean-hatrie-tmp-metadata
clean-hatrie-tmp-metadata:
	bash ./scripts/audit-hatrie-tmp.sh clean-metadata
stage-clean-hatrie-tmp-metadata:
	bash ./scripts/stage-clean-hatrie-tmp-metadata.sh
commit-clean-hatrie-tmp-metadata:
	bash ./scripts/commit-clean-hatrie-tmp-metadata.sh
push-clean-hatrie-tmp-metadata:
	bash ./scripts/push-clean-hatrie-tmp-metadata.sh
EOF

makefile_blob=$(git hash-object -w "$tmp_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git add -- \
  scripts/stage-clean-hatrie-tmp-metadata.sh \
  scripts/commit-clean-hatrie-tmp-metadata.sh \
  scripts/push-clean-hatrie-tmp-metadata.sh
git diff --cached --check
printf 'Staged cleanup metadata delivery paths:\n'
git diff --cached --name-only --
