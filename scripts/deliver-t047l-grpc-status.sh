#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
mode="${1:-status}"
commit_message="feat: add T047l gRPC participant status [skip ci]"
work_file="$(mktemp /tmp/t047l-delivery.XXXXXX)"
head_file="$(mktemp /tmp/t047l-head.XXXXXX)"
desired_file="$(mktemp /tmp/t047l-desired.XXXXXX)"
raw_patch="$(mktemp /tmp/t047l-raw-patch.XXXXXX)"
cached_patch="$(mktemp /tmp/t047l-cached-patch.XXXXXX)"
addition_file="$(mktemp /tmp/t047l-addition.XXXXXX)"
trap 'rm -f -- "$work_file" "$head_file" "$desired_file" "$raw_patch" "$cached_patch" "$addition_file"' EXIT

feature_paths=(
  Makefile
  BENCHMARK.md
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  INSPIRATION.md
  README.md
  T047L_GRPC_STATUS.md
  hat/hatCache/grpc_cluster_write_commit.go
  hat/hatCache/tu47_cluster_write_commit_grpc_benchmark_test.go
  hat/hatCache/tu47_cluster_write_commit_grpc_test.go
  internal/gen/hatriecache/v1/cache.pb.go
  proto/hatriecache/v1/cache.proto
  scripts/deliver-t047l-grpc-status.sh
)

show_status() {
  printf '%s\n' '--- staged paths ---'
  git -C "$repo_root" diff --cached --name-status
  printf '%s\n' '--- feature paths ---'
  git -C "$repo_root" status --short -- "${feature_paths[@]}"
}

ensure_no_overlapping_staged_changes() {
  local staged path
  staged="$(git -C "$repo_root" diff --cached --name-only)"
  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    for feature_path in "${feature_paths[@]}"; do
      if [[ "$path" == "$feature_path" ]]; then
        printf 'Refusing to overwrite an existing staged change at %s.\n' "$path" >&2
        exit 1
      fi
    done
  done <<< "$staged"
}

write_diff_patch() {
  local path="$1"
  local diff_status=0
  git diff --no-index --unified=3 "$head_file" "$desired_file" > "$raw_patch" || diff_status=$?
  if [[ "$diff_status" -ne 1 ]]; then
    printf 'Could not construct isolated patch for %s (status %s).\n' "$path" "$diff_status" >&2
    exit 1
  fi
  sed \
    -e "s|a${head_file}|a/$path|g" \
    -e "s|b${desired_file}|b/$path|g" \
    "$raw_patch" > "$cached_patch"
  git -C "$repo_root" apply --cached --whitespace=nowarn "$cached_patch"
}

stage_inserted_file() {
  local path="$1"
  local marker="$2"
  local added_marker="$3"
  git -C "$repo_root" show "HEAD:$path" > "$head_file"
  if grep -Fq -- "$added_marker" "$head_file"; then
    return 0
  fi
  if ! grep -Fq -- "$marker" "$path"; then
    printf 'Expected working-tree marker is missing from %s.\n' "$path" >&2
    exit 1
  fi
  awk -v marker="$marker" -v addition="$addition_file" '
    {
      print
      if (!inserted && index($0, marker)) {
        while ((getline line < addition) > 0) print line
        inserted = 1
      }
    }
    END {
      close(addition)
      if (!inserted) exit 3
    }
  ' "$head_file" > "$desired_file"
  write_diff_patch "$path"
}

stage_makefile_target() {
  git -C "$repo_root" show HEAD:Makefile > "$head_file"
  if grep -Fq 'deliver-t047l-grpc-status:' "$head_file"; then
    return 0
  fi
  cat "$head_file" > "$desired_file"
  printf '%s\n' \
    '' \
    '.PHONY: deliver-t047l-grpc-status' \
    'deliver-t047l-grpc-status:' \
    $'\tbash ./scripts/deliver-t047l-grpc-status.sh deliver' \
    >> "$desired_file"
  write_diff_patch Makefile
}

stage_feature() {
  ensure_no_overlapping_staged_changes
  git -C "$repo_root" add -- \
    T047L_GRPC_STATUS.md \
    hat/hatCache/grpc_cluster_write_commit.go \
    hat/hatCache/tu47_cluster_write_commit_grpc_benchmark_test.go \
    hat/hatCache/tu47_cluster_write_commit_grpc_test.go \
    internal/gen/hatriecache/v1/cache.pb.go \
    proto/hatriecache/v1/cache.proto \
    scripts/deliver-t047l-grpc-status.sh

  printf '%s\n' '- [x] T047l Authenticated gRPC participant status reads over the existing cluster-write RPC; see [T047L_GRPC_STATUS.md](T047L_GRPC_STATUS.md).' > "$addition_file"
  stage_inserted_file INSPIRATION.md '- [x] T047k Authenticated HTTP participant status reads.' '- [x] T047l Authenticated gRPC participant status reads.'
  printf '%s\n' '- [x] T047l Authenticated gRPC participant status reads over the existing cluster-write RPC; see [T047L_GRPC_STATUS.md](T047L_GRPC_STATUS.md).' > "$addition_file"
  stage_inserted_file CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md '- [x] T047k Authenticated HTTP participant status reads' '- [x] T047l Authenticated gRPC participant status reads'
  printf '%s\n' '- Opt-in authenticated gRPC participant status reads over the existing cluster-write RPC: [T047L_GRPC_STATUS.md](T047L_GRPC_STATUS.md)' > "$addition_file"
  stage_inserted_file README.md '- Opt-in authenticated HTTP participant status reads for T047 recovery:' '- Opt-in authenticated gRPC participant status reads over the existing cluster-write RPC:'
  printf '%s\n' \
    '' \
    '<a id="t047l-grpc-status"></a>' \
    '## T047l gRPC participant status' \
    '' \
    'Command: `make benchmark-t047-grpc-transport`.' \
    '' \
    'Measured on Linux/amd64, AMD Ryzen 9 5950X, five samples per subbenchmark:' \
    '' \
    '| Path | Median ns/op | B/op | allocs/op | Relative latency |' \
    '| --- | ---: | ---: | ---: | ---: |' \
    '| Direct participant `Status` | 19.60 | 0 | 0 | 1.00x |' \
    '| Authenticated gRPC `Status` | 32,664 | 12,492 | 175 | 1,667x direct latency |' \
    '| Existing gRPC prepare+commit | 62,168 | 24,530 | 348 | status is 1.90x lower |' \
    '' \
    'Status is a read-only recovery operation over the existing authenticated' \
    '`ClusterWriteCommit` RPC. Compared with the existing prepare+commit pair, it' \
    'uses 1.96x fewer response-path bytes and 1.99x fewer allocations. The cost is' \
    'one authenticated unary round trip and the additional proposal metadata in a' \
    'found response; normal write phases and their wire contract are unchanged.' \
    > "$addition_file"
  stage_inserted_file BENCHMARK.md '<a id="t047k-http-status"></a>' '<a id="t047l-grpc-status"></a>'
  stage_makefile_target
  git -C "$repo_root" diff --cached --check -- \
    T047L_GRPC_STATUS.md \
    hat/hatCache/grpc_cluster_write_commit.go \
    hat/hatCache/tu47_cluster_write_commit_grpc_benchmark_test.go \
    hat/hatCache/tu47_cluster_write_commit_grpc_test.go \
    internal/gen/hatriecache/v1/cache.pb.go \
    proto/hatriecache/v1/cache.proto \
    scripts/deliver-t047l-grpc-status.sh
  printf '%s\n' '--- staged feature paths ---'
  git -C "$repo_root" diff --cached --name-status -- "${feature_paths[@]}"
}

commit_feature() {
  stage_feature
  git -C "$repo_root" commit -m "$commit_message"
}

push_feature() {
  git -C "$repo_root" push origin HEAD
}

unstage_feature() {
  git -C "$repo_root" restore --staged -- \
    BENCHMARK.md \
    CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
    INSPIRATION.md \
    Makefile \
    README.md \
    T047L_GRPC_STATUS.md \
    hat/hatCache/grpc_cluster_write_commit.go \
    hat/hatCache/tu47_cluster_write_commit_grpc_benchmark_test.go \
    hat/hatCache/tu47_cluster_write_commit_grpc_test.go \
    internal/gen/hatriecache/v1/cache.pb.go \
    proto/hatriecache/v1/cache.proto \
    scripts/deliver-t047l-grpc-status.sh
}

case "$mode" in
  status)
    show_status
    ;;
  stage)
    stage_feature
    ;;
  commit)
    commit_feature
    ;;
  push)
    push_feature
    ;;
  unstage)
    unstage_feature
    ;;
  deliver)
    commit_feature
    push_feature
    ;;
  *)
    printf 'Usage: %s {status|stage|commit|push|deliver|unstage}\n' "$0" >&2
    exit 2
    ;;
esac
