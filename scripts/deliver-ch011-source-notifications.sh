#!/usr/bin/env bash
set -euo pipefail

mode=${1:-deliver}
case "$mode" in
  stage|commit|push|deliver) ;;
  *) printf 'usage: %s [stage|commit|push|deliver]\n' "$0" >&2; exit 2 ;;
esac

files=(
  CH011_SOURCE_NOTIFICATIONS.md
  hat/hatSql/ch011_source_notifications.go
  hat/hatSql/ch011_source_notifications_baseline_test.go
  hat/hatSql/ch011_source_notifications_test.go
  scripts/benchmark-ch011-source-notifications.sh
  scripts/format-ch011-source-notifications.sh
  scripts/race-ch011-source-notifications.sh
  scripts/test-ch011-source-notifications-package.sh
  scripts/test-ch011-source-notifications.sh
  scripts/verify-ch011-source-notifications.sh
  scripts/vet-ch011-source-notifications.sh
  scripts/deliver-ch011-source-notifications.sh
)

if [[ -n "$(git diff --cached --name-only)" ]]; then
  printf '%s\n' 'refusing to stage CH-011 while unrelated changes are already staged' >&2
  exit 1
fi

for file in "${files[@]}"; do
  [[ -e "$file" ]] || { printf 'missing delivery file: %s\n' "$file" >&2; exit 1; }
done

makefile=$(mktemp)
trap 'rm -f "$makefile"' EXIT
git show HEAD:Makefile > "$makefile"
printf '%s\n' \
  '' \
  '.PHONY: test-ch011-source-notifications benchmark-ch011-source-notifications format-ch011-source-notifications' \
  'test-ch011-source-notifications:' \
  $'\t@bash scripts/test-ch011-source-notifications.sh' \
  'benchmark-ch011-source-notifications:' \
  $'\t@bash scripts/benchmark-ch011-source-notifications.sh' \
  'format-ch011-source-notifications:' \
  $'\t@bash scripts/format-ch011-source-notifications.sh' \
  '.PHONY: race-ch011-source-notifications vet-ch011-source-notifications test-ch011-source-notifications-package verify-ch011-source-notifications' \
  'race-ch011-source-notifications:' \
  $'\t@bash scripts/race-ch011-source-notifications.sh' \
  'vet-ch011-source-notifications:' \
  $'\t@bash scripts/vet-ch011-source-notifications.sh' \
  'test-ch011-source-notifications-package:' \
  $'\t@bash scripts/test-ch011-source-notifications-package.sh' \
  'verify-ch011-source-notifications:' \
  $'\t@bash scripts/verify-ch011-source-notifications.sh' \
  'deliver-ch011-source-notifications:' \
  $'\t@bash scripts/deliver-ch011-source-notifications.sh deliver' >> "$makefile"

git diff --check -- "${files[@]}"
git add -- "${files[@]}"
blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo 100644 "$blob" Makefile
git diff --cached --check

case "$mode" in
  stage) exit 0 ;;
esac

git commit -m 'feat: add event-driven materialized view refresh queue [skip ci]'
case "$mode" in
  commit) exit 0 ;;
esac

git push origin HEAD
