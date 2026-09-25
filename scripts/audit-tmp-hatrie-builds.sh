#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
case "$mode" in
  preview|clean) ;;
  *)
    printf 'usage: %s [preview|clean]\n' "$0" >&2
    exit 2
    ;;
esac

tmp_root=/tmp
plan=/tmp/hatrie-cache-tmp-builds.plan
current_worktree=$(pwd -P)
min_age_seconds=${HATRIE_TMP_MIN_AGE_SECONDS:-86400}

if [[ "$mode" == "preview" ]]; then
  : > "$plan"
  now=$(date +%s)
  candidate_count=0
  review_count=0

  printf 'Top-level Hatrie build inventory under %s (age >= %ss):\n' "$tmp_root" "$min_age_seconds"
  printf 'Protected: active worktrees, any path containing .git, and live process working directories.\n'
  printf 'Review-only: generic go-build*/go-link* paths are listed but never removed.\n'

  while IFS= read -r -d '' path; do
    name=${path##*/}
    case "$name" in
      hatrie-cache-tmp-builds.plan|hatrie-cache-tmp-cleanup.plan) continue ;;
    esac
    category=hatrie
    case "$name" in
      *hatrie*|*Hatrie*) ;;
      go-build*|go-link*) category=generic-go-build ;;
      *) continue ;;
    esac

    mtime=$(stat -c '%Y' -- "$path")
    age=$((now - mtime))
    ((age < 0)) && age=0
    size=$(du -sh -- "$path" 2>/dev/null | awk '{print $1}')

    if [[ "$category" == generic-go-build ]]; then
      review_count=$((review_count + 1))
      printf 'REVIEW-ONLY %-16s age=%-8ss size=%-8s path=%s\n' "$category" "$age" "$size" "$path"
      continue
    fi

    protected=0
    reason=
    if [[ "$path" == "$current_worktree" ]]; then
      protected=1
      reason=active-worktree
    elif [[ -e "$path/.git" ]]; then
      protected=1
      reason=git-metadata
    else
      for proc_dir in /proc/[0-9]*; do
        [[ -d "$proc_dir" ]] || continue
        proc_cwd=$(readlink -f "$proc_dir/cwd" 2>/dev/null || true)
        case "$proc_cwd" in
          "$path"|"$path"/*)
            protected=1
            reason=live-process-cwd
            break
            ;;
        esac
      done
    fi

    if ((protected)); then
      printf 'PROTECTED %-18s age=%-8ss size=%-8s path=%s\n' "$reason" "$age" "$size" "$path"
    elif ((age < min_age_seconds)); then
      printf 'RECENT %-21s age=%-8ss size=%-8s path=%s\n' "skip" "$age" "$size" "$path"
    else
      printf '%s\n' "$path" >> "$plan"
      candidate_count=$((candidate_count + 1))
      printf 'CANDIDATE %-17s age=%-8ss size=%-8s path=%s\n' "stale-hatrie-build" "$age" "$size" "$path"
    fi
  done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 -print0 | sort -z)

  printf 'Summary: %s candidate(s), %s review-only generic Go path(s).\n' "$candidate_count" "$review_count"
  if ((candidate_count == 0)); then
    rm -f -- "$plan"
    printf 'Plan: none\n'
  else
    printf 'Plan: %s\n' "$plan"
  fi
  exit 0
fi

if [[ ! -s "$plan" ]]; then
  printf 'No reviewed cleanup plan at %s; run preview first.\n' "$plan"
  exit 0
fi

removed=0
while IFS= read -r path; do
  [[ -n "$path" ]] || continue
  case "$path" in
    /tmp/*) ;;
    *)
      printf 'refusing unexpected cleanup path: %s\n' "$path" >&2
      exit 1
      ;;
  esac
  if [[ "$path" == "$current_worktree" || -e "$path/.git" ]]; then
    printf 'refusing protected cleanup path: %s\n' "$path" >&2
    exit 1
  fi
  printf 'Removing %s\n' "$path"
  rm -rf -- "$path"
  removed=$((removed + 1))
done < "$plan"
rm -f -- "$plan"
printf 'Removed %s reviewed stale Hatrie build path(s).\n' "$removed"
