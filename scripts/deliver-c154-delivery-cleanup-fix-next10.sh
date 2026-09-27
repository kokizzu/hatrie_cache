#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_paths=(
  "Makefile"
  "scripts/deliver-c154-replication-rollout-next10.sh"
  "scripts/test-c154-delivery-cleanup-next10.sh"
  "scripts/deliver-c154-delivery-cleanup-fix-next10.sh"
)
readonly make_marker="# C154_REPLICATION_SCHEMA_ROLLOUT_DELIVERY_NEXT10_BEGIN"
followup_tmp_dir=""

cleanup_followup_tmp_dir() {
  if [[ -n "$followup_tmp_dir" ]]; then
    rm -rf -- "$followup_tmp_dir"
  fi
}

die() {
  printf 'delivery error: %s\n' "$*" >&2
  exit 1
}

assert_no_feature_paths_staged() {
  if ! git diff --cached --quiet -- "${feature_paths[@]}"; then
    die "one or more cleanup-fix paths already have staged changes"
  fi
}

make_no_index_patch() {
  local base="$1"
  local target="$2"
  local patch_path="$3"
  local rc

  set +e
  git diff --no-index --binary --src-prefix=a/ --dst-prefix=b/ "$base" "$target" >"$patch_path"
  rc=$?
  set -e
  case "$rc" in
    0) : >"$patch_path" ;;
    1) ;;
    *) die "could not create the Makefile cleanup-fix patch" ;;
  esac
}

stage_feature() {
  assert_no_feature_paths_staged
  local tmp_dir
  followup_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154-cleanup-fix.XXXXXX")"
  tmp_dir="$followup_tmp_dir"
  trap cleanup_followup_tmp_dir EXIT

  git add -- \
    scripts/deliver-c154-replication-rollout-next10.sh \
    scripts/test-c154-delivery-cleanup-next10.sh \
    scripts/deliver-c154-delivery-cleanup-fix-next10.sh

  local base="$tmp_dir/Makefile.base"
  local target="$tmp_dir/Makefile.target"
  local section="$tmp_dir/Makefile.section"
  local patch_path="$tmp_dir/Makefile.patch"

  grep -Fqx -- "test-c154-delivery-cleanup-next10:" Makefile || die "cleanup regression target is missing"
  git show HEAD:Makefile >"$base"
  printf '%s\n' \
    "test-c154-delivery-cleanup-next10:" \
    $'\t@bash scripts/test-c154-delivery-cleanup-next10.sh' \
    >"$section"
  awk -v marker="$make_marker" -v section="$section" '
    !inserted && $0 == marker {
      while ((getline line < section) > 0) print line
      close(section)
      inserted = 1
    }
    { print }
    END { if (!inserted) exit 2 }
  ' "$base" >"$target" || die "cleanup delivery marker is missing from HEAD Makefile"

  make_no_index_patch "$base" "$target" "$patch_path"
  sed -i \
    -e "s|a/$base|a/Makefile|g" \
    -e "s|b/$target|b/Makefile|g" \
    -e "s|a/${base#/}|a/Makefile|g" \
    -e "s|b/${target#/}|b/Makefile|g" \
    "$patch_path"
  git apply --cached --check "$patch_path"
  git apply --cached "$patch_path"
  git diff --cached --check
  git diff --cached --stat -- "${feature_paths[@]}"
  git diff --cached --name-status -- "${feature_paths[@]}"
}

commit_feature() {
  git diff --cached --check
  git commit -m "fix: clean replication rollout delivery temp files [skip ci]"
}

push_feature() {
  git push origin HEAD
}

case "${1:-status}" in
  status)
    git status --short --branch
    git diff --cached --name-status -- "${feature_paths[@]}"
    ;;
  stage)
    stage_feature
    ;;
  commit)
    commit_feature
    ;;
  push)
    push_feature
    ;;
  deliver)
    stage_feature
    commit_feature
    push_feature
    ;;
  *)
    die "usage: $0 {status|stage|commit|push|deliver}"
    ;;
esac
