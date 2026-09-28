#!/usr/bin/env bash
set -euo pipefail

action="${1:-plan}"
plan_file=/tmp/hatrie-cache-tmp-cleanup.plan
preserved=/tmp/hatrie-cache-inspiration-next10

case "$action" in
plan)
  find /tmp -mindepth 1 -maxdepth 1 \( -name 'hatrie*' -o -name 'hatri*' \) -print \
    | while IFS= read -r path; do
        case "$path" in
          "$preserved"|"$plan_file") ;;
          *) printf '%s\n' "$path" ;;
        esac
      done \
    | sort >"$plan_file"
  printf '%s\n' "Cleanup plan: $plan_file"
  if [[ -s "$plan_file" ]]; then
    while IFS= read -r path; do
      printf 'REMOVE %s\n' "$path"
    done <"$plan_file"
  else
    printf '%s\n' 'REMOVE none'
  fi
  printf '%s\n' "PRESERVE $preserved"
  ;;
apply)
  if [[ ! -f "$plan_file" ]]; then
    printf '%s\n' "missing cleanup plan: $plan_file" >&2
    exit 1
  fi
  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    case "$path" in
      /tmp/hatrie*|/tmp/hatri*) ;;
      *) printf '%s\n' "unsafe cleanup path: $path" >&2; exit 1 ;;
    esac
    case "$path" in
      "$preserved"|"$plan_file") printf '%s\n' "unsafe preserved cleanup path: $path" >&2; exit 1 ;;
    esac
    rm -rf -- "$path"
  done <"$plan_file"
  rm -f -- "$plan_file"
  printf '%s\n' "Cleanup applied; preserved $preserved"
  ;;
*)
  printf '%s\n' "usage: $0 plan|apply" >&2
  exit 2
  ;;
esac
