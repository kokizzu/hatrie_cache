#!/usr/bin/env bash
set -euo pipefail

mode=${1:-inventory}
tmp_root=/tmp
min_age_minutes=${HATRIE_TMP_MIN_AGE_MINUTES:-1440}
plan_file=${HATRIE_TMP_INVENTORY_PLAN:-.hatrie-tmp-inventory.plan}

is_protected() {
  local path=$1
  case "$path" in
    "$tmp_root/hatrie-cache-next-feature"|"$tmp_root/hatrie-cache-next-goal")
      return 0
      ;;
    "$tmp_root/hatrie-cache-next-feature"/*|"$tmp_root/hatrie-cache-next-goal"/*)
      return 0
      ;;
  esac
  [[ -e "$path/.git" ]]
}

matches_name() {
  local path=$1
  local name=${path##*/}
  [[ "$name" == *hatrie* || "$name" == go-build* || "$name" == *hatrie*build* ]]
}

collect_candidates() {
  find "$tmp_root" -xdev -mindepth 1 -maxdepth 3 -type d -mmin +"$min_age_minutes" -print0 2>/dev/null |
    while IFS= read -r -d '' path; do
      matches_name "$path" || continue
      is_protected "$path" && continue
      printf '%s\n' "$path"
    done
}

collect_named_entries() {
  find "$tmp_root" -xdev -mindepth 1 -maxdepth 3 \( -iname '*hatrie*' -o -iname '*build*' -o -iname 'go-*' \) -print0 2>/dev/null |
    while IFS= read -r -d '' path; do
      is_protected "$path" && continue
      printf '%s\n' "$path"
    done
}

case "$mode" in
  inventory)
    echo "Hatrie/build temporary inventory: $tmp_root"
    echo "Age filter: >= ${min_age_minutes} minutes"
    count=0
    while IFS= read -r path; do
      count=$((count + 1))
      du -sh "$path" 2>/dev/null || true
    done < <(collect_candidates)
    echo "Candidate count: $count"
    ;;
  inventory-named)
    echo "Named Hatrie/build temporary inventory: $tmp_root"
    count=0
    while IFS= read -r path; do
      count=$((count + 1))
      du -sh "$path" 2>/dev/null || true
    done < <(collect_named_entries)
    echo "Named entry count: $count"
    ;;
  preview)
    : > "$plan_file"
    while IFS= read -r path; do
      printf '%s\n' "$path" >> "$plan_file"
    done < <(collect_candidates)
    echo "Cleanup preview plan: $plan_file"
    if [[ -s "$plan_file" ]]; then
      while IFS= read -r path; do
        du -sh "$path" 2>/dev/null || true
      done < "$plan_file"
    else
      echo "Plan: none"
    fi
    ;;
  clean)
    [[ -f "$plan_file" ]] || { echo "Missing preview plan: $plan_file" >&2; exit 1; }
    while IFS= read -r path; do
      [[ -n "$path" ]] || continue
      [[ -d "$path" ]] || { echo "Skipping missing path: $path"; continue; }
      is_protected "$path" && { echo "Refusing protected path: $path" >&2; exit 1; }
      echo "Removing: $path"
      rm -rf -- "$path"
    done < "$plan_file"
    rm -f -- "$plan_file"
    ;;
  prune-empty-plans)
    for plan in .hatrie-tmp-cleanup.plan .hatrie-tmp-inventory.plan; do
      if [[ -f "$plan" ]] && ! awk '!/^[[:space:]]*(#|$)/ { found=1 } END { exit found ? 0 : 1 }' "$plan"; then
        echo "Removing empty generated plan: $plan"
        rm -f -- "$plan"
      fi
    done
    ;;
  inspect-plan)
    if [[ -f .hatrie-tmp-cleanup.plan ]]; then
      echo "Cleanup plan: .hatrie-tmp-cleanup.plan"
      nl -ba .hatrie-tmp-cleanup.plan
    else
      echo "Cleanup plan: none"
    fi
    ;;
  *)
    echo "usage: $0 inventory|inventory-named|preview|clean|prune-empty-plans|inspect-plan" >&2
    exit 2
    ;;
esac
