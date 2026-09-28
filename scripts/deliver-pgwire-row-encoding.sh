#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

feature_paths=(
  Makefile
  PGWIRE_ROW_ENCODING_REUSE.md
  hat/hatPgWire/server.go
  hat/hatPgWire/backend_row_encoding_reuse_test.go
  hat/hatPgWire/backend_row_encoding_reuse_benchmark_test.go
  scripts/format-pgwire-row-encoding.sh
  scripts/test-pgwire-row-encoding.sh
  scripts/benchmark-pgwire-row-encoding-baseline.sh
  scripts/benchmark-pgwire-row-encoding.sh
  scripts/race-pgwire-row-encoding.sh
  scripts/deliver-pgwire-row-encoding.sh
)

commit_message='feat(pgwire): encode data rows in reusable buffers [skip ci]'

makefile_block() {
  cat <<'EOF'

.PHONY: format-pgwire-row-encoding test-pgwire-row-encoding benchmark-pgwire-row-encoding-baseline benchmark-pgwire-row-encoding race-pgwire-row-encoding
format-pgwire-row-encoding:
	bash scripts/format-pgwire-row-encoding.sh
test-pgwire-row-encoding:
	bash scripts/test-pgwire-row-encoding.sh
benchmark-pgwire-row-encoding-baseline:
	bash scripts/benchmark-pgwire-row-encoding-baseline.sh
benchmark-pgwire-row-encoding:
	bash scripts/benchmark-pgwire-row-encoding.sh
race-pgwire-row-encoding:
	bash scripts/race-pgwire-row-encoding.sh

.PHONY: stage-pgwire-row-encoding commit-pgwire-row-encoding push-pgwire-row-encoding
stage-pgwire-row-encoding:
	bash scripts/deliver-pgwire-row-encoding.sh stage
commit-pgwire-row-encoding:
	bash scripts/deliver-pgwire-row-encoding.sh commit
push-pgwire-row-encoding:
	bash scripts/deliver-pgwire-row-encoding.sh push
EOF
}

stage_makefile() {
  local index_file=$1 makefile_snapshot makefile_blob
  makefile_snapshot=$(mktemp /tmp/hatrie-pgwire-row-makefile.XXXXXX)
  git show HEAD:Makefile > "$makefile_snapshot"
  if ! grep -q '^stage-pgwire-row-encoding:' "$makefile_snapshot"; then
    makefile_block >> "$makefile_snapshot"
  fi
  makefile_blob=$(git hash-object -w "$makefile_snapshot")
  GIT_INDEX_FILE="$index_file" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
  rm -f -- "$makefile_snapshot"
}

stage_feature() {
  local index_file
  index_file=$(mktemp /tmp/hatrie-pgwire-row-index.XXXXXX)
  rm -f -- "$index_file"
  trap "rm -f -- '$index_file'" EXIT

  GIT_INDEX_FILE="$index_file" git read-tree HEAD
  for path in "${feature_paths[@]}"; do
    if [[ "$path" != Makefile ]]; then
      GIT_INDEX_FILE="$index_file" git add -- "$path"
    fi
  done
  stage_makefile "$index_file"
  GIT_INDEX_FILE="$index_file" git diff --cached --check

  mapfile -t staged_paths < <(GIT_INDEX_FILE="$index_file" git diff --cached --name-only)
  for path in "${staged_paths[@]}"; do
    case "$path" in
      Makefile|PGWIRE_ROW_ENCODING_REUSE.md|hat/hatPgWire/server.go|hat/hatPgWire/backend_row_encoding_reuse_test.go|hat/hatPgWire/backend_row_encoding_reuse_benchmark_test.go|scripts/format-pgwire-row-encoding.sh|scripts/test-pgwire-row-encoding.sh|scripts/benchmark-pgwire-row-encoding-baseline.sh|scripts/benchmark-pgwire-row-encoding.sh|scripts/race-pgwire-row-encoding.sh|scripts/deliver-pgwire-row-encoding.sh)
        ;;
      *)
        echo "unexpected staged path: $path" >&2
        return 1
        ;;
    esac
  done
  printf '%s\n' "${staged_paths[@]}"
  GIT_INDEX_FILE="$index_file" git diff --cached --stat
}

commit_feature() {
  local index_file
  index_file=$(mktemp /tmp/hatrie-pgwire-row-index.XXXXXX)
  rm -f -- "$index_file"
  trap "rm -f -- '$index_file'" EXIT

  GIT_INDEX_FILE="$index_file" git read-tree HEAD
  for path in "${feature_paths[@]}"; do
    if [[ "$path" != Makefile ]]; then
      GIT_INDEX_FILE="$index_file" git add -- "$path"
    fi
  done
  stage_makefile "$index_file"
  GIT_INDEX_FILE="$index_file" git diff --cached --check
  GIT_INDEX_FILE="$index_file" git commit -m "$commit_message"
}

push_feature() {
  local local_commit remote_commit tmp_worktree
  local_commit=$(git rev-parse HEAD)
  git fetch origin master
  remote_commit=$(git rev-parse origin/master)

  if git merge-base --is-ancestor "$remote_commit" "$local_commit"; then
    git push origin HEAD:master
    return 0
  fi

  tmp_worktree=$(mktemp -d /tmp/hatrie-pgwire-row-push.XXXXXX)
  cleanup_worktree() {
    local worktree_path=$1
    git worktree remove --force "$worktree_path" >/dev/null 2>&1 || true
    git worktree prune >/dev/null 2>&1 || true
  }
  trap "cleanup_worktree '$tmp_worktree'" EXIT

  git worktree add --detach "$tmp_worktree" "$remote_commit"
  if ! git -C "$tmp_worktree" cherry-pick "$local_commit"; then
    mapfile -t conflicts < <(git -C "$tmp_worktree" diff --name-only --diff-filter=U)
    for path in "${conflicts[@]}"; do
      if [[ "$path" != Makefile ]]; then
        echo "unresolved PGWire row delivery conflict: $path" >&2
        git -C "$tmp_worktree" cherry-pick --abort
        return 1
      fi
    done
    git -C "$tmp_worktree" checkout --ours -- Makefile
    if ! grep -q '^stage-pgwire-row-encoding:' "$tmp_worktree/Makefile"; then
      makefile_block >> "$tmp_worktree/Makefile"
    fi
    git -C "$tmp_worktree" add -- Makefile
    GIT_EDITOR=true git -C "$tmp_worktree" cherry-pick --continue
  fi
  git -C "$tmp_worktree" push origin HEAD:master
}

case "${1:-}" in
  stage)
    stage_feature
    ;;
  commit)
    commit_feature
    ;;
  push)
    push_feature
    ;;
  *)
    echo "usage: $0 {stage|commit|push}" >&2
    exit 2
    ;;
esac
