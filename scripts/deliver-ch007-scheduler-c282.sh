#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
commit_message='adopt background TTL scheduler and durable deadlines [skip ci]'

feature_paths=(
  CH007_TTL_SCHEDULER.md
  CH007_ROW_TTL.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  README.md
  hat/hatSql/typed_table_ttl_scheduler.go
  hat/hatSql/typed_table_ttl_snapshot.go
  hat/hatSql/ch007_ttl_scheduler_test.go
  hat/hatSql/ch007_ttl_scheduler_baseline_benchmark_test.go
  hat/hatSql/ch007_ttl_scheduler_benchmark_test.go
  scripts/test-ch007-scheduler-c282.sh
  scripts/benchmark-before-ch007-scheduler-c282.sh
  scripts/benchmark-ch007-scheduler-c282.sh
  scripts/format-ch007-scheduler-c282.sh
  scripts/verify-ch007-scheduler-c282.sh
  scripts/verify-ch007-docs-c284.sh
  scripts/deliver-ch007-scheduler-c282.sh
)

die() {
  printf 'deliver-ch007: %s\n' "$*" >&2
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
      printf '\n.PHONY: review-ch007-scheduler-c282 stage-ch007-scheduler-c282 inspect-staged-ch007-scheduler-c282 commit-ch007-scheduler-c282 push-ch007-scheduler-c282\nreview-ch007-scheduler-c282:\n\t@bash scripts/deliver-ch007-scheduler-c282.sh review\nstage-ch007-scheduler-c282:\n\t@bash scripts/deliver-ch007-scheduler-c282.sh stage\ninspect-staged-ch007-scheduler-c282:\n\t@bash scripts/deliver-ch007-scheduler-c282.sh inspect\ncommit-ch007-scheduler-c282:\n\t@bash scripts/deliver-ch007-scheduler-c282.sh commit\npush-ch007-scheduler-c282:\n\t@bash scripts/deliver-ch007-scheduler-c282.sh push\n' >> "$tmp_dir/feature.mk"
      git diff --no-index --src-prefix=a/ --dst-prefix=b/ "$tmp_dir/base.mk" "$tmp_dir/feature.mk" > "$tmp_dir/makefile.diff" || diff_status=$?
      diff_status=${diff_status:-0}
      if [[ "$diff_status" -gt 1 ]]; then
        die "could not inspect Makefile changes"
      fi
      if [[ "$diff_status" -eq 1 ]]; then
        base_path=${tmp_dir}/base.mk
        feature_path=${tmp_dir}/feature.mk
        sed -i "s#${base_path}#Makefile#g; s#${feature_path}#Makefile#g" "$tmp_dir/makefile.diff"
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
    die 'usage: deliver-ch007-scheduler-c282.sh {review|stage|inspect|commit|push}'
    ;;
esac
