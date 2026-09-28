#!/usr/bin/env bash
set -euo pipefail

readonly feature_script='scripts/cleanup-hatrie-tmp-full.sh'
readonly delivery_script='scripts/deliver-hatrie-tmp-cleanup.sh'
readonly makefile='Makefile'
readonly commit_message='chore: add conservative Hatrie tmp cleanup [skip ci]'

readonly makefile_block='\n.PHONY: cleanup-hatrie-tmp-full-plan cleanup-hatrie-tmp-full-apply cleanup-hatrie-tmp-full-audit cleanup-hatrie-tmp-full-self-test cleanup-hatrie-tmp-full-self-test-fixtures\ncleanup-hatrie-tmp-full-plan:\n\tbash scripts/cleanup-hatrie-tmp-full.sh plan\n\ncleanup-hatrie-tmp-full-apply:\n\tbash scripts/cleanup-hatrie-tmp-full.sh apply\n\ncleanup-hatrie-tmp-full-audit:\n\tbash scripts/cleanup-hatrie-tmp-full.sh audit\n\ncleanup-hatrie-tmp-full-self-test:\n\tbash scripts/cleanup-hatrie-tmp-full.sh self-test\n\ncleanup-hatrie-tmp-full-self-test-fixtures:\n\tbash scripts/cleanup-hatrie-tmp-full.sh cleanup-self-test-fixtures\n\n.PHONY: stage-hatrie-tmp-full-cleanup commit-hatrie-tmp-full-cleanup push-hatrie-tmp-full-cleanup status-hatrie-tmp-full-cleanup\nstage-hatrie-tmp-full-cleanup:\n\tbash scripts/deliver-hatrie-tmp-cleanup.sh stage\n\ncommit-hatrie-tmp-full-cleanup:\n\tbash scripts/deliver-hatrie-tmp-cleanup.sh commit\n\npush-hatrie-tmp-full-cleanup:\n\tbash scripts/deliver-hatrie-tmp-cleanup.sh push\n\nstatus-hatrie-tmp-full-cleanup:\n\tbash scripts/deliver-hatrie-tmp-cleanup.sh status\n'

usage() {
  printf 'usage: %s status|stage|commit|push\n' "$0"
}

status() {
  git status --short
}

stage_makefile() {
  local staged_makefile blob mode temp_file
  temp_file="$(mktemp)"
  trap 'rm -f -- "${temp_file:-}"' RETURN
  git show HEAD:Makefile > "$temp_file"
  printf '%b' "$makefile_block" >> "$temp_file"
  blob="$(git hash-object -w --path="$makefile" "$temp_file")"
  mode="$(git ls-tree HEAD -- "$makefile" | cut -d' ' -f1)"
  git update-index --add --cacheinfo "$mode,$blob,$makefile"
  staged_makefile="$(git diff --cached --name-only -- "$makefile")"
  [[ "$staged_makefile" == "$makefile" ]] || {
    printf 'failed to stage generated %s\n' "$makefile" >&2
    exit 1
  }
}

verify_staged() {
  local unexpected path
  while IFS= read -r path; do
    case "$path" in
      "$feature_script"|"$delivery_script"|"$makefile") ;;
      *)
        unexpected=1
        printf 'unexpected staged path: %s\n' "$path" >&2
        ;;
    esac
  done < <(git diff --cached --name-only)
  [[ -z "${unexpected:-}" ]] || exit 1
}

stage() {
  if ! git diff --cached --quiet; then
    verify_staged
    git reset -- "$feature_script" "$delivery_script" "$makefile"
  fi
  git add -- "$feature_script" "$delivery_script"
  stage_makefile
  verify_staged
  git diff --cached --stat
}

commit() {
  if git diff --cached --quiet; then
    stage
  else
    verify_staged
  fi
  git commit -m "$commit_message"
}

push() {
  git push
}

main() {
  [[ "$#" -eq 1 ]] || {
    usage >&2
    exit 2
  }
  case "$1" in
    status) status ;;
    stage) stage ;;
    commit) commit ;;
    push) push ;;
    *) usage >&2; exit 2 ;;
  esac
}

main "$@"
