#!/usr/bin/env bash
set -euo pipefail

feature_files=(
  INSPIRATION.md
  README.md
  hat/hatCache/replication.go
  hat/hatCache/replication_task_ownership_test.go
  scripts/format-t115.sh
  scripts/test-race-t115.sh
  scripts/test-t115.sh
  scripts/vet-t115.sh
  scripts/review-t115.sh
  scripts/stage-t115.sh
  scripts/commit-t115.sh
  scripts/push-t115.sh
)

for path in "${feature_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    printf 'missing T115 file: %s\n' "$path" >&2
    exit 1
  fi
done

index_file=$(mktemp)
desired_file=$(mktemp)
patch_file=$(mktemp)
cleanup() {
  rm -f "$index_file" "$desired_file" "$patch_file"
}
trap cleanup EXIT

git show :Makefile >"$index_file"
if grep -q '^test-t115:' "$index_file"; then
  :
else
  cat "$index_file" >"$desired_file"
  cat >>"$desired_file" <<'EOF'

test-t115:
	bash ./scripts/test-t115.sh

format-t115:
	bash ./scripts/format-t115.sh

test-race-t115:
	bash ./scripts/test-race-t115.sh

vet-t115:
	bash ./scripts/vet-t115.sh

review-t115:
	bash ./scripts/review-t115.sh

stage-t115:
	bash ./scripts/stage-t115.sh

commit-t115:
	bash ./scripts/commit-t115.sh

push-t115:
	bash ./scripts/push-t115.sh
EOF
  diff -u --label a/Makefile --label b/Makefile "$index_file" "$desired_file" >"$patch_file" || diff_status=$?
  if [[ "${diff_status:-0}" != 1 ]]; then
    printf 'unexpected Makefile patch status: %s\n' "${diff_status:-0}" >&2
    exit 1
  fi
  git apply --cached "$patch_file"
fi

git add -- "${feature_files[@]}"
git diff --cached --check
