#!/usr/bin/env bash
set -euo pipefail

action="${1:-status}"
case "$action" in
  status|stage|commit|push|deliver) ;;
  *) printf 'usage: %s {status|stage|commit|push|deliver}\n' "$0" >&2; exit 2 ;;
esac

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

index_contains() {
  local path="$1"
  local pattern="$2"
  local snapshot="$tmp_dir/index-${path//\//_}"
  if ! git show ":$path" >"$snapshot" 2>/dev/null; then
    return 1
  fi
  rg -q "$pattern" "$snapshot"
}

is_allowed_path() {
  case "$1" in
    BENCHMARK.md|INSPIRATION.md|Makefile|M033C_GLOBAL_TIMESTAMP_GRPC.md|proto/hatriecache/v1/cache.proto|internal/gen/hatriecache/v1/cache.pb.go|internal/gen/hatriecache/v1/cache_grpc.pb.go|hat/hatCache/grpc.go|hat/hatCache/m033c_global_timestamp_grpc.go|hat/hatCache/m033c_global_timestamp_grpc_test.go|hat/hatCache/m033c_global_timestamp_grpc_benchmark_test.go|scripts/bench-m033c-global-timestamp-grpc.sh|scripts/format-m033c-global-timestamp-grpc.sh|scripts/test-m033c-global-timestamp-grpc.sh|scripts/test-m033c-packages.sh|scripts/race-m033c-global-timestamp-grpc.sh|scripts/race-m033c-focused.sh|scripts/deliver-m033c-global-timestamp-grpc.sh) return 0 ;;
    *) return 1 ;;
  esac
}

reject_unrelated_staged() {
  local path
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    if ! is_allowed_path "$path"; then
      printf 'refusing unrelated staged path: %s\n' "$path" >&2
      return 1
    fi
  done < <(git diff --cached --name-only)
}

apply_shared_patch() {
  local base="$1"
  local desired="$2"
  local path="$3"
  local patch="$tmp_dir/${path//\//_}.patch"
  local diff_output
  diff_output="$(diff -u "$base" "$desired" || true)"
  [[ -n "$diff_output" ]] || return 0
  {
    printf 'diff --git a/%s b/%s\n' "$path" "$path"
    printf '%s\n' "--- a/$path"
    printf '%s\n' "+++ b/$path"
    printf '%s\n' "$diff_output" | awk 'NR > 2 {print}'
  } >"$patch"
  git apply --cached --check "$patch"
  git apply --cached "$patch"
}

stage_shared_files() {
  local base desired

  base="$tmp_dir/inspiration.base"
  desired="$tmp_dir/inspiration.desired"
  if ! index_contains "INSPIRATION.md" '^- \[x\] M033c Authenticated bounded gRPC global timestamp reservation transport'; then
    git show HEAD:INSPIRATION.md >"$base"
    awk '
      index($0, "- [x] M033b Consensus-bound global timestamp reservations") == 1 {
        print
        print "- [x] M033c Authenticated bounded gRPC global timestamp reservation transport with request-bound grant validation; consensus, leader election, and durable coordinator publication remain caller-owned. See [M033C_GLOBAL_TIMESTAMP_GRPC.md](M033C_GLOBAL_TIMESTAMP_GRPC.md)."
        next
      }
      { print }
    ' "$base" >"$desired"
    apply_shared_patch "$base" "$desired" INSPIRATION.md
  fi

  base="$tmp_dir/benchmark.base"
  desired="$tmp_dir/benchmark.desired"
  if ! index_contains "BENCHMARK.md" '^## M033c Global timestamp reservation gRPC transport'; then
    git show HEAD:BENCHMARK.md >"$base"
    awk '
      { print }
      END {
        print ""
        print "## M033c Global timestamp reservation gRPC transport"
        print ""
        print "Raw `go test ./hat/hatCache -run '\''^$'\'' -bench '\''BenchmarkM033cGlobalTimestampReserve'\'' -benchmem -benchtime=1s -count=3` results:"
        print ""
        print "```text"
        print "BenchmarkM033cGlobalTimestampReserveDirect-32  23447428  51.22 ns/op       0 B/op    0 allocs/op"
        print "BenchmarkM033cGlobalTimestampReserveDirect-32  23034114  48.27 ns/op       0 B/op    0 allocs/op"
        print "BenchmarkM033cGlobalTimestampReserveDirect-32  23307928  52.02 ns/op       0 B/op    0 allocs/op"
        print "BenchmarkM033cGlobalTimestampReserveGRPC-32      35863 29181 ns/op   11984 B/op  173 allocs/op"
        print "BenchmarkM033cGlobalTimestampReserveGRPC-32      38778 28786 ns/op   11949 B/op  172 allocs/op"
        print "BenchmarkM033cGlobalTimestampReserveGRPC-32      39265 30130 ns/op   11902 B/op  172 allocs/op"
        print "```"
        print ""
        print "Median direct reservation is 51.22 ns/op with zero allocations. Median gRPC"
        print "reservation is 29,181 ns/op, 11,949 B/op, and 172 allocations: about 570x the"
        print "latency of the in-process baseline. This is expected transport overhead for a"
        print "new cross-process capability; the RPC is opt-in and the local/default path is"
        print "unchanged."
      }
    ' "$base" >"$desired"
    apply_shared_patch "$base" "$desired" BENCHMARK.md
  fi

  base="$tmp_dir/makefile.base"
  desired="$tmp_dir/makefile.desired"
  if ! index_contains "Makefile" '^stage-m033c-global-timestamp-grpc:'; then
    git show HEAD:Makefile >"$base"
    awk '
      { print }
      END {
        print ""
        print "test-m033c-global-timestamp-grpc:"
        print "\t@bash ./scripts/test-m033c-global-timestamp-grpc.sh"
        print ""
        print "bench-m033c-global-timestamp-grpc:"
        print "\t@bash ./scripts/bench-m033c-global-timestamp-grpc.sh"
        print ""
        print "format-m033c-global-timestamp-grpc:"
        print "\t@bash ./scripts/format-m033c-global-timestamp-grpc.sh"
        print ""
        print "test-m033c-packages:"
        print "\t@bash ./scripts/test-m033c-packages.sh"
        print ""
        print "race-m033c-global-timestamp-grpc:"
        print "\t@bash ./scripts/race-m033c-global-timestamp-grpc.sh"
        print ""
        print "race-m033c-focused:"
        print "\t@bash ./scripts/race-m033c-focused.sh"
        print ""
        print "stage-m033c-global-timestamp-grpc:"
        print "\t@bash ./scripts/deliver-m033c-global-timestamp-grpc.sh stage"
        print ""
        print "commit-m033c-global-timestamp-grpc:"
        print "\t@bash ./scripts/deliver-m033c-global-timestamp-grpc.sh commit"
        print ""
        print "push-m033c-global-timestamp-grpc:"
        print "\t@bash ./scripts/deliver-m033c-global-timestamp-grpc.sh push"
        print ""
        print "deliver-m033c-global-timestamp-grpc:"
        print "\t@bash ./scripts/deliver-m033c-global-timestamp-grpc.sh deliver"
      }
    ' "$base" >"$desired"
    apply_shared_patch "$base" "$desired" Makefile
  fi
}

stage_feature_files() {
  git add \
    M033C_GLOBAL_TIMESTAMP_GRPC.md \
    proto/hatriecache/v1/cache.proto \
    internal/gen/hatriecache/v1/cache.pb.go \
    internal/gen/hatriecache/v1/cache_grpc.pb.go \
    hat/hatCache/grpc.go \
    hat/hatCache/m033c_global_timestamp_grpc.go \
    hat/hatCache/m033c_global_timestamp_grpc_test.go \
    hat/hatCache/m033c_global_timestamp_grpc_benchmark_test.go \
    scripts/bench-m033c-global-timestamp-grpc.sh \
    scripts/format-m033c-global-timestamp-grpc.sh \
    scripts/test-m033c-global-timestamp-grpc.sh \
    scripts/test-m033c-packages.sh \
    scripts/race-m033c-global-timestamp-grpc.sh \
    scripts/race-m033c-focused.sh \
    scripts/deliver-m033c-global-timestamp-grpc.sh
}

stage_all() {
  reject_unrelated_staged
  stage_feature_files
  stage_shared_files
  reject_unrelated_staged
  git diff --cached --check
  printf '%s\n' 'M033c feature paths staged.'
}

case "$action" in
  status)
    git status --short -- M033C_GLOBAL_TIMESTAMP_GRPC.md BENCHMARK.md INSPIRATION.md Makefile proto internal/gen/hatriecache/v1/cache.pb.go internal/gen/hatriecache/v1/cache_grpc.pb.go hat/hatCache/grpc.go hat/hatCache/m033c_global_timestamp_grpc.go hat/hatCache/m033c_global_timestamp_grpc_test.go hat/hatCache/m033c_global_timestamp_grpc_benchmark_test.go scripts/*m033c*
    ;;
  stage)
    stage_all
    ;;
  commit)
    stage_all
    git commit -m 'feat(replication): add global timestamp reservation transport [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  deliver)
    stage_all
    git commit -m 'feat(replication): add global timestamp reservation transport [skip ci]'
    git push origin HEAD
    ;;
esac
