#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

feature_paths=(
  Makefile
  PGWIRE_BACKEND_BUFFER_REUSE.md
  hat/hatPgWire/server.go
  hat/hatPgWire/backend_message_buffer_reuse_test.go
  hat/hatPgWire/backend_message_buffer_reuse_benchmark_test.go
  scripts/format-pgwire-response-buffer.sh
  scripts/test-pgwire-response-buffer.sh
  scripts/benchmark-pgwire-response-buffer-baseline.sh
  scripts/benchmark-pgwire-response-buffer.sh
  scripts/race-pgwire-response-buffer.sh
  scripts/deliver-pgwire-response-buffer.sh
)

commit_message='feat(pgwire): reuse backend response buffers [skip ci]'

makefile_block() {
  cat <<'EOF'

.PHONY: format-pgwire-response-buffer test-pgwire-response-buffer benchmark-pgwire-response-buffer-baseline benchmark-pgwire-response-buffer race-pgwire-response-buffer
format-pgwire-response-buffer:
	bash scripts/format-pgwire-response-buffer.sh
test-pgwire-response-buffer:
	bash scripts/test-pgwire-response-buffer.sh
benchmark-pgwire-response-buffer-baseline:
	bash scripts/benchmark-pgwire-response-buffer-baseline.sh
benchmark-pgwire-response-buffer:
	bash scripts/benchmark-pgwire-response-buffer.sh
race-pgwire-response-buffer:
	bash scripts/race-pgwire-response-buffer.sh

.PHONY: stage-pgwire-response-buffer commit-pgwire-response-buffer push-pgwire-response-buffer
stage-pgwire-response-buffer:
	bash scripts/deliver-pgwire-response-buffer.sh stage
commit-pgwire-response-buffer:
	bash scripts/deliver-pgwire-response-buffer.sh commit
push-pgwire-response-buffer:
	bash scripts/deliver-pgwire-response-buffer.sh push
EOF
}

stage_makefile() {
  local index_file=$1 makefile_snapshot makefile_blob
  makefile_snapshot=$(mktemp /tmp/hatrie-pgwire-response-makefile.XXXXXX)
  git show HEAD:Makefile > "$makefile_snapshot"
  if ! grep -q '^stage-pgwire-response-buffer:' "$makefile_snapshot"; then
    makefile_block >> "$makefile_snapshot"
  fi
  makefile_blob=$(git hash-object -w "$makefile_snapshot")
  GIT_INDEX_FILE="$index_file" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
  rm -f -- "$makefile_snapshot"
}

stage_feature() {
  local index_file
  index_file=$(mktemp /tmp/hatrie-pgwire-response-index.XXXXXX)
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
      Makefile|PGWIRE_BACKEND_BUFFER_REUSE.md|hat/hatPgWire/server.go|hat/hatPgWire/backend_message_buffer_reuse_test.go|hat/hatPgWire/backend_message_buffer_reuse_benchmark_test.go|scripts/format-pgwire-response-buffer.sh|scripts/test-pgwire-response-buffer.sh|scripts/benchmark-pgwire-response-buffer-baseline.sh|scripts/benchmark-pgwire-response-buffer.sh|scripts/race-pgwire-response-buffer.sh|scripts/deliver-pgwire-response-buffer.sh)
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
  index_file=$(mktemp /tmp/hatrie-pgwire-response-index.XXXXXX)
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

  tmp_worktree=$(mktemp -d /tmp/hatrie-pgwire-response-push.XXXXXX)
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
        echo "unresolved PGWire response delivery conflict: $path" >&2
        git -C "$tmp_worktree" cherry-pick --abort
        return 1
      fi
    done
    git -C "$tmp_worktree" checkout --ours -- Makefile
    if ! grep -q '^stage-pgwire-response-buffer:' "$tmp_worktree/Makefile"; then
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
