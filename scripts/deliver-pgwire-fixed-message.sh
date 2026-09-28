#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

feature_paths=(
  Makefile
  PGWIRE_FIXED_MESSAGE_REUSE.md
  hat/hatPgWire/server.go
  hat/hatPgWire/backend_fixed_message_reuse_test.go
  hat/hatPgWire/backend_fixed_message_reuse_benchmark_test.go
  scripts/format-pgwire-fixed-message.sh
  scripts/test-pgwire-fixed-message.sh
  scripts/benchmark-pgwire-fixed-message-baseline.sh
  scripts/benchmark-pgwire-fixed-message.sh
  scripts/race-pgwire-fixed-message.sh
  scripts/deliver-pgwire-fixed-message.sh
)

commit_message='feat(pgwire): reuse fixed control messages [skip ci]'

makefile_block() {
  cat <<'EOF'

.PHONY: format-pgwire-fixed-message test-pgwire-fixed-message benchmark-pgwire-fixed-message-baseline benchmark-pgwire-fixed-message race-pgwire-fixed-message
format-pgwire-fixed-message:
	bash scripts/format-pgwire-fixed-message.sh
test-pgwire-fixed-message:
	bash scripts/test-pgwire-fixed-message.sh
benchmark-pgwire-fixed-message-baseline:
	bash scripts/benchmark-pgwire-fixed-message-baseline.sh
benchmark-pgwire-fixed-message:
	bash scripts/benchmark-pgwire-fixed-message.sh
race-pgwire-fixed-message:
	bash scripts/race-pgwire-fixed-message.sh

.PHONY: stage-pgwire-fixed-message commit-pgwire-fixed-message push-pgwire-fixed-message
stage-pgwire-fixed-message:
	bash scripts/deliver-pgwire-fixed-message.sh stage
commit-pgwire-fixed-message:
	bash scripts/deliver-pgwire-fixed-message.sh commit
push-pgwire-fixed-message:
	bash scripts/deliver-pgwire-fixed-message.sh push
EOF
}

stage_makefile() {
  local index_file=$1 makefile_snapshot makefile_blob
  makefile_snapshot=$(mktemp /tmp/hatrie-pgwire-fixed-makefile.XXXXXX)
  git show HEAD:Makefile > "$makefile_snapshot"
  if ! grep -q '^stage-pgwire-fixed-message:' "$makefile_snapshot"; then
    makefile_block >> "$makefile_snapshot"
  fi
  makefile_blob=$(git hash-object -w "$makefile_snapshot")
  GIT_INDEX_FILE="$index_file" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
  rm -f -- "$makefile_snapshot"
}

stage_feature() {
  local index_file
  index_file=$(mktemp /tmp/hatrie-pgwire-fixed-index.XXXXXX)
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
      Makefile|PGWIRE_FIXED_MESSAGE_REUSE.md|hat/hatPgWire/server.go|hat/hatPgWire/backend_fixed_message_reuse_test.go|hat/hatPgWire/backend_fixed_message_reuse_benchmark_test.go|scripts/format-pgwire-fixed-message.sh|scripts/test-pgwire-fixed-message.sh|scripts/benchmark-pgwire-fixed-message-baseline.sh|scripts/benchmark-pgwire-fixed-message.sh|scripts/race-pgwire-fixed-message.sh|scripts/deliver-pgwire-fixed-message.sh)
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
  index_file=$(mktemp /tmp/hatrie-pgwire-fixed-index.XXXXXX)
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

  tmp_worktree=$(mktemp -d /tmp/hatrie-pgwire-fixed-push.XXXXXX)
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
        echo "unresolved PGWire fixed-message delivery conflict: $path" >&2
        git -C "$tmp_worktree" cherry-pick --abort
        return 1
      fi
    done
    git -C "$tmp_worktree" checkout --ours -- Makefile
    if ! grep -q '^stage-pgwire-fixed-message:' "$tmp_worktree/Makefile"; then
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
