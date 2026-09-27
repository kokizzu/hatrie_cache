#!/usr/bin/env bash
set -euo pipefail

mode=${1:?expected status, stage, commit, push, or deliver}

case "$mode" in
  status)
    git status --short
    ;;
  stage)
    if ! git diff --cached --quiet; then
      printf 'refusing to stage CH052 while the index already contains changes\n' >&2
      exit 1
    fi

    temp_dir=$(mktemp -d /tmp/hatrie-cache-ch052-delivery.XXXXXX)
    cleanup() {
      rm -rf "$temp_dir"
    }
    trap cleanup EXIT

    git show HEAD:Makefile > "$temp_dir/base"
    cp "$temp_dir/base" "$temp_dir/feature"
    printf '\n%s\n' \
      '.PHONY: format-ch052-semi-anti-join test-ch052-semi-anti-join benchmark-ch052-semi-anti-join' \
      'format-ch052-semi-anti-join:' \
      $'\t@bash scripts/test-ch052-semi-anti-join.sh format' \
      '' \
      'test-ch052-semi-anti-join:' \
      $'\t@bash scripts/test-ch052-semi-anti-join.sh test' \
      '' \
      'benchmark-ch052-semi-anti-join:' \
      $'\t@bash scripts/test-ch052-semi-anti-join.sh benchmark' \
      '.PHONY: test-ch052-hatsql' \
      'test-ch052-hatsql:' \
      $'\t@bash scripts/test-ch052-semi-anti-join.sh package' \
      '' \
      '.PHONY: race-ch052-semi-anti-join' \
      'race-ch052-semi-anti-join:' \
      $'\t@bash scripts/test-ch052-semi-anti-join.sh race' \
      '' \
      '.PHONY: vet-ch052-semi-anti-join' \
      'vet-ch052-semi-anti-join:' \
      $'\t@bash scripts/test-ch052-semi-anti-join.sh vet' \
      '' \
      '.PHONY: stage-ch052-semi-anti-join commit-ch052-semi-anti-join push-ch052-semi-anti-join deliver-ch052-semi-anti-join' \
      'stage-ch052-semi-anti-join:' \
      $'\t@bash scripts/deliver-ch052-semi-anti-join.sh stage' \
      'commit-ch052-semi-anti-join:' \
      $'\t@bash scripts/deliver-ch052-semi-anti-join.sh commit' \
      'push-ch052-semi-anti-join:' \
      $'\t@bash scripts/deliver-ch052-semi-anti-join.sh push' \
      'deliver-ch052-semi-anti-join:' \
      $'\t@bash scripts/deliver-ch052-semi-anti-join.sh deliver' \
      >> "$temp_dir/feature"
    git diff --no-index -- "$temp_dir/base" "$temp_dir/feature" > "$temp_dir/makefile.patch" || true
    sed -i \
      -e "s|a$temp_dir/base|a/Makefile|g" \
      -e "s|b$temp_dir/feature|b/Makefile|g" \
      "$temp_dir/makefile.patch"
    git apply --cached "$temp_dir/makefile.patch"

    git add -- \
      CH052_SEMI_ANTI_JOIN.md \
      hat/hatSql/ch052_semi_anti_join_test.go \
      hat/hatSql/query.go \
      hat/hatSql/semi_join.go \
      scripts/deliver-ch052-semi-anti-join.sh \
      scripts/test-ch052-semi-anti-join.sh
    git diff --cached --check
    git diff --cached --stat
    ;;
  commit)
    git diff --cached --quiet && {
      printf 'nothing staged for CH052\n' >&2
      exit 1
    }
    git commit -m 'feat(sql): add semi and anti joins [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  deliver)
    "$0" stage
    "$0" commit
    "$0" push
    ;;
  *)
    printf 'unsupported mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
