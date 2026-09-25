#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  RESTORE_RESUME.md \
  hat/hatCache/backup_restore.go \
  hat/hatCache/backup_restore_resume_checkpoint.go \
  hat/hatCache/backup_restore_resume_checkpoint_benchmark_test.go \
  hat/hatCache/backup_restore_resume_test.go \
  scripts/benchmark-mz003-resume-checkpoint.sh \
  scripts/format-mz003-resume-checkpoint.sh \
  scripts/stage-mz003-resume-checkpoint.sh \
  scripts/commit-mz003-resume-checkpoint.sh \
  scripts/push-mz003-resume-checkpoint.sh

expected=(
  Makefile
  BENCHMARK.md
  ENGINE_IDEAS.md
  RESTORE_RESUME.md
  hat/hatCache/backup_restore.go
  hat/hatCache/backup_restore_resume_checkpoint.go
  hat/hatCache/backup_restore_resume_checkpoint_benchmark_test.go
  hat/hatCache/backup_restore_resume_test.go
  scripts/benchmark-mz003-resume-checkpoint.sh
  scripts/format-mz003-resume-checkpoint.sh
  scripts/stage-mz003-resume-checkpoint.sh
  scripts/commit-mz003-resume-checkpoint.sh
  scripts/push-mz003-resume-checkpoint.sh
)
mapfile -t staged < <(git diff --cached --name-only)
if [[ "${#staged[@]}" -ne "${#expected[@]}" ]]; then
  printf 'unexpected staged path set:\n' >&2
  printf '  %s\n' "${staged[@]}" >&2
  exit 1
fi
for path in "${staged[@]}"; do
  found=0
  for allowed in "${expected[@]}"; do
    if [[ "$path" == "$allowed" ]]; then
      found=1
      break
    fi
  done
  if [[ "$found" != 1 ]]; then
    printf 'unexpected staged path: %s\n' "$path" >&2
    exit 1
  fi
done

git diff --cached --check
git diff --cached --stat
