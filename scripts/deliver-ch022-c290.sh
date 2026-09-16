#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
commit_message='adopt durable incremental backup manifest catalog [skip ci]'

feature_paths=(
  CH022_INCREMENTAL_PART_BACKUP.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  README.md
  BENCHMARK.md
  hat/hatBackup/encryption.go
  hat/hatBackup/object_store.go
  hat/hatBackup/ch022_catalog_integration_test.go
  hat/hatBackup/ch022_catalog_backup_benchmark_test.go
  hat/hatBackup/ch022_catalog_backup_baseline_benchmark_test.go
  scripts/benchmark-before-ch022-c290.sh
  scripts/benchmark-ch022-c290.sh
  scripts/format-ch022-c290.sh
  scripts/verify-ch022-c290.sh
  scripts/deliver-ch022-c290.sh
)

die() {
  printf 'deliver-ch022: %s\n' "$*" >&2
  exit 1
}

case "$mode" in
  review)
    git status --short
    git diff --stat -- "${feature_paths[@]}"
    git diff --stat -- Makefile
    ;;
  stage)
    staged=$(git diff --cached --name-only)
    makefile_staged=false
    if [[ -n "$staged" ]]; then
      while IFS= read -r path; do
        if [[ "$path" == Makefile ]]; then
          makefile_staged=true
          continue
        fi
        case " ${feature_paths[*]} Makefile " in
          *" $path "*) ;;
          *) die "unrelated path is already staged: $path" ;;
        esac
      done <<< "$staged"
    fi

    git add -- "${feature_paths[@]}"

    if [[ "$makefile_staged" == false ]]; then
      tmp_dir=$(mktemp -d)
      trap 'rm -rf "$tmp_dir"' EXIT
      git show HEAD:Makefile > "$tmp_dir/base.mk"
      cp "$tmp_dir/base.mk" "$tmp_dir/feature.mk"
      printf '\n.PHONY: benchmark-before-ch022-c290 benchmark-ch022-c290 format-ch022-c290 verify-ch022-c290\nbenchmark-before-ch022-c290:\n\t@bash scripts/benchmark-before-ch022-c290.sh\nbenchmark-ch022-c290:\n\t@bash scripts/benchmark-ch022-c290.sh\nformat-ch022-c290:\n\t@bash scripts/format-ch022-c290.sh\nverify-ch022-c290:\n\t@bash scripts/verify-ch022-c290.sh\n' >> "$tmp_dir/feature.mk"
      (cd "$tmp_dir" && git diff --no-index --src-prefix=a/ --dst-prefix=b/ base.mk feature.mk > makefile.diff) || diff_status=$?
      diff_status=${diff_status:-0}
      if [[ "$diff_status" -gt 1 ]]; then
        die 'could not inspect Makefile changes'
      fi
      if [[ "$diff_status" -eq 1 ]]; then
        sed -i 's#a/base.mk#a/Makefile#g; s#b/feature.mk#b/Makefile#g' "$tmp_dir/makefile.diff"
        git apply --cached -- "$tmp_dir/makefile.diff"
      fi
    fi
    ;;
  inspect)
    git diff --cached --check
    git diff --cached --stat
    git diff --cached --name-only
    git diff --cached -- Makefile
    ;;
  commit)
    git diff --cached --check
    git diff --cached --quiet && die 'nothing is staged'
    git commit -m "$commit_message"
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    die 'usage: deliver-ch022-c290.sh {review|stage|inspect|commit|push}'
    ;;
esac
