#!/usr/bin/env bash
set -euo pipefail

mode="${1:-plan}"
plan_file="${HATRIE_TMP_PLAN:-/tmp/hatrie-cache-cleanup-plan.tsv}"
entries_file="${plan_file}.entries"
worktrees_file="${plan_file}.worktrees"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

is_protected() {
  [[ "$1" == "$plan_file" || "$1" == "$entries_file" || "$1" == "$worktrees_file" || "$1" == "$repo_root" ]]
}

is_registered_worktree() {
  local candidate="$1"
  local line
  while IFS= read -r line; do
    case "$line" in
      worktree\ *)
        [[ "${line#worktree }" == "$candidate" ]] && return 0
        ;;
    esac
  done <"$worktrees_file"
  return 1
}

case "$mode" in
  plan)
    [[ "$plan_file" == /tmp/* ]] || {
      printf 'refusing plan outside /tmp: %s\n' "$plan_file" >&2
      exit 1
    }
    umask 077
    : >"$plan_file"
    git worktree list --porcelain >"$worktrees_file"
    find /tmp -mindepth 1 -maxdepth 1 -print0 >"$entries_file"
    printf 'Hatrie temporary cleanup plan: %s\n' "$plan_file"
    found=0
    while IFS= read -r -d '' path; do
      base="${path##*/}"
      case "$base" in
        hatrie*|hatrie_cache*)
          found=1
          if is_protected "$path"; then
            printf 'KEEP\t%s\n' "$path"
          elif is_registered_worktree "$path"; then
            printf 'REMOVE_WORKTREE\t%s\n' "$path" >>"$plan_file"
            printf 'REMOVE_WORKTREE\t%s\n' "$path"
          else
            printf 'REMOVE_PATH\t%s\n' "$path" >>"$plan_file"
            printf 'REMOVE_PATH\t%s\n' "$path"
          fi
          ;;
      esac
    done <"$entries_file"
    [[ "$found" -eq 1 ]] || printf 'no Hatrie-named temporary paths\n'
    printf 'Top-level generic go-build directories are not selected automatically:\n'
    find /tmp -mindepth 1 -maxdepth 1 -type d -name 'go-build*' -print
    rm -f -- "$entries_file" "$worktrees_file"
    ;;
  apply)
    [[ -f "$plan_file" ]] || {
      printf 'missing cleanup plan: %s\n' "$plan_file" >&2
      exit 1
    }
    while IFS=$'\t' read -r action path; do
      [[ -n "$action" ]] || continue
      [[ "$path" == /tmp/* ]] || {
        printf 'refusing path outside /tmp: %s\n' "$path" >&2
        exit 1
      }
      [[ "${path##*/}" == *[hH][aA][tT][rR][iI][eE]* ]] || {
        printf 'refusing path without Hatrie name: %s\n' "$path" >&2
        exit 1
      }
      is_protected "$path" && {
        printf 'refusing protected path: %s\n' "$path" >&2
        exit 1
      }
      case "$action" in
        REMOVE_WORKTREE)
          git worktree remove --force "$path"
          ;;
        REMOVE_PATH)
          rm -rf -- "$path"
          ;;
        *)
          printf 'unknown cleanup action: %s\n' "$action" >&2
          exit 1
          ;;
      esac
    done <"$plan_file"
    rm -f -- "$plan_file"
    printf 'Applied and removed the cleanup plan.\n'
    ;;
  *)
    printf 'usage: %s plan|apply\n' "$0" >&2
    exit 2
    ;;
esac
