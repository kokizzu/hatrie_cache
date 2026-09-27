#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

feature_files=(
  ENGINE_INSPIRATION_150.md
  scripts/verify-engine-inspiration.sh
  scripts/deliver-engine-inspiration.sh
)

check_staged_scope() {
  local path
  while IFS= read -r path; do
    case "$path" in
      ENGINE_INSPIRATION_150.md|scripts/verify-engine-inspiration.sh|scripts/deliver-engine-inspiration.sh|Makefile) ;;
      '') ;;
      *) printf 'refusing unrelated staged path: %s\n' "$path" >&2; exit 1 ;;
    esac
  done < <(git diff --cached --name-only)
}

cleanup_stage_files() {
  rm -f "$repo_root"/.engine-inspiration-stage.*
}

write_selective_patch() {
  local patch_file="$1"
  cat > "$patch_file" <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -28632,3 +28632,19 @@
 test-tt024-package:
 @@TAB@@bash ./scripts/test-tt024-package.sh
@@PLUS@@.PHONY: verify-engine-inspiration
@@PLUS@@verify-engine-inspiration:
@@PLUS@@@@TAB@@bash ./scripts/verify-engine-inspiration.sh
@@PLUS@@.PHONY: stage-engine-inspiration commit-engine-inspiration push-engine-inspiration deliver-engine-inspiration status-engine-inspiration unstage-engine-inspiration
@@PLUS@@stage-engine-inspiration:
@@PLUS@@@@TAB@@bash ./scripts/deliver-engine-inspiration.sh stage
@@PLUS@@commit-engine-inspiration:
@@PLUS@@@@TAB@@bash ./scripts/deliver-engine-inspiration.sh commit
@@PLUS@@push-engine-inspiration:
@@PLUS@@@@TAB@@bash ./scripts/deliver-engine-inspiration.sh push
@@PLUS@@deliver-engine-inspiration:
@@PLUS@@@@TAB@@bash ./scripts/deliver-engine-inspiration.sh deliver
@@PLUS@@status-engine-inspiration:
@@PLUS@@@@TAB@@bash ./scripts/deliver-engine-inspiration.sh status
@@PLUS@@unstage-engine-inspiration:
@@PLUS@@@@TAB@@bash ./scripts/deliver-engine-inspiration.sh unstage
 .PHONY: test-tt024-materialized-text
PATCH
  sed -i $'s/@@TAB@@/\t/g; s/@@PLUS@@/+/g' "$patch_file"
}

stage_feature() {
  local patch_file
  cleanup_stage_files
  check_staged_scope
  if ! git diff --cached --quiet; then
    printf 'refusing to stage with existing staged changes\n' >&2
    exit 1
  fi
  git add -- "${feature_files[@]}"
  patch_file="$(mktemp "$repo_root/.engine-inspiration-stage.XXXXXX")"
  write_selective_patch "$patch_file"
  git apply --cached --check "$patch_file"
  git apply --cached "$patch_file"
  rm -f "$patch_file"
  git diff --cached --check
  check_staged_scope
  git diff --cached --stat
  cleanup_stage_files
}

status_feature() {
  check_staged_scope
  git status --short -- "${feature_files[@]}" Makefile
  git diff --cached --stat -- "${feature_files[@]}" Makefile
}

unstage_feature() {
  local path
  check_staged_scope
  while IFS= read -r path; do
    [ -n "$path" ] || continue
    git restore --staged -- "$path"
  done < <(git diff --cached --name-only -- "${feature_files[@]}" Makefile)
  cleanup_stage_files
}

commit_feature() {
  if git diff --cached --quiet; then
    stage_feature
  else
    check_staged_scope
    git diff --cached --check
  fi
  git commit -m "docs: catalog engine-inspired gaps [skip ci]"
}

push_feature() {
  git push
}

case "${1:-}" in
  stage) stage_feature ;;
  status) status_feature ;;
  unstage) unstage_feature ;;
  commit) commit_feature ;;
  push) push_feature ;;
  deliver) commit_feature; push_feature ;;
  *) printf 'usage: %s {stage|status|unstage|commit|push|deliver}\n' "$0" >&2; exit 2 ;;
esac
