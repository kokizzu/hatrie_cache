#!/usr/bin/env bash
set -euo pipefail

action="${1:-status}"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153g-delivery.XXXXXX")"
trap 'rm -rf -- "$tmp_dir"' EXIT

feature_files=(
  C153_PARTITION_OWNERSHIP_GRPC.md
  proto/hatriecache/v1/cache.proto
  internal/gen/hatriecache/v1/cache.pb.go
  internal/gen/hatriecache/v1/cache_grpc.pb.go
  hat/hatCache/c153g_partition_ownership_consensus_grpc.go
  hat/hatCache/c153g_partition_ownership_consensus_grpc_test.go
  hat/hatCache/c153g_partition_ownership_consensus_grpc_benchmark_test.go
  scripts/bench-c153g-ownership-grpc.sh
  scripts/format-c153g-ownership-grpc.sh
  scripts/race-c153g-ownership-grpc.sh
  scripts/test-c153g-all.sh
  scripts/test-c153g-ownership-grpc.sh
  scripts/test-c153g-packages.sh
  scripts/verify-c153g-ownership-grpc.sh
  scripts/deliver-c153g-ownership-grpc.sh
)

is_allowed() {
  local path="$1"
  [[ "$path" == "Makefile" || "$path" == "BENCHMARK.md" || "$path" == "INSPIRATION.md" || "$path" == "hat/hatCache/grpc.go" ]] && return 0
  for allowed in "${feature_files[@]}"; do
    [[ "$path" == "$allowed" ]] && return 0
  done
  return 1
}

assert_no_unrelated_staged() {
  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    if ! is_allowed "$path"; then
      printf 'unrelated staged path blocks delivery: %s\n' "$path" >&2
      exit 1
    fi
  done < <(git diff --cached --name-only)
}

apply_diff_from_base() {
  local base="$1"
  local desired="$2"
  local path="$3"
  local patch="$tmp_dir/$(basename "$path").patch"
  local diff_output
  diff_output="$(diff -u "$base" "$desired" || true)"
  {
    printf 'diff --git a/%s b/%s\n' "$path" "$path"
    printf '%s\n' "--- a/$path" "+++ b/$path"
    printf '%s\n' "$diff_output" | tail -n +3
  } > "$patch"
  git apply --cached --check "$patch"
  git apply --cached "$patch"
}

stage_shared_files() {
  local base desired

  index_contains() {
    local path="$1"
    local pattern="$2"
    local snapshot="$tmp_dir/index-${path//\//_}"
    if ! git show ":$path" >"$snapshot" 2>/dev/null; then
      return 1
    fi
    rg -q "$pattern" "$snapshot"
  }

  if index_contains "hat/hatCache/grpc.go" 'PartitionOwnershipConsensusVote PartitionOwnershipConsensusVoteHandler'; then
    :
  else
  base="$tmp_dir/grpc.base"
  desired="$tmp_dir/grpc.desired"
  git show HEAD:hat/hatCache/grpc.go > "$base"
  if ! rg -q 'PartitionOwnershipConsensusVote PartitionOwnershipConsensusVoteHandler' "$base"; then
    awk '
      { print }
      /ClusterWriteCommitParticipant \*hatReplication\.ClusterWriteCommitParticipant$/ {
        print "\t// PartitionOwnershipConsensusVote enables the opt-in gRPC ownership vote"
        print "\t// transport. Nil keeps the RPC unavailable and does not alter normal"
        print "\t// command handling."
        print "\tPartitionOwnershipConsensusVote PartitionOwnershipConsensusVoteHandler"
        print "\t// PartitionOwnershipConsensusAuthenticator optionally verifies handler"
        print "\t// votes before they are returned to a caller-owned quorum collector."
        print "\tPartitionOwnershipConsensusAuthenticator PartitionOwnershipConsensusVoteAuthenticator"
      }
    ' "$base" > "$desired"
    apply_diff_from_base "$base" "$desired" hat/hatCache/grpc.go
  fi
  fi

  base="$tmp_dir/inspiration.base"
  desired="$tmp_dir/inspiration.desired"
  if index_contains "INSPIRATION.md" '^- \[x\] C153 Metadata consensus for partition ownership\.'; then
    :
  else
  git show HEAD:INSPIRATION.md > "$base"
  if ! rg -q '^- \[x\] C153 Metadata consensus for partition ownership\.' "$base"; then
    awk '
      index($0, "- [ ] C153 Metadata consensus for partition ownership.") == 1 {
        print "- [x] C153 Metadata consensus for partition ownership. Quorum admission, validated ownership metadata, durable snapshot/restore, authenticated bounded gRPC vote transport, and a reusable gRPC collector adapter are implemented; automatic topology publication and durable control-plane coordination remain caller-owned. See [C153_PARTITION_OWNERSHIP_GRPC.md](C153_PARTITION_OWNERSHIP_GRPC.md)."
        next
      }
      { print }
    ' "$base" > "$desired"
    apply_diff_from_base "$base" "$desired" INSPIRATION.md
  fi
  fi

  base="$tmp_dir/benchmark.base"
  desired="$tmp_dir/benchmark.desired"
  if index_contains "BENCHMARK.md" '^## C153g Partition-ownership consensus vote transport'; then
    :
  else
  git show HEAD:BENCHMARK.md > "$base"
  if ! rg -q '^## C153g Partition-ownership consensus vote transport' "$base"; then
    cp "$base" "$desired"
    cat >> "$desired" <<'EOF'

## C153g Partition-ownership consensus vote transport

`make bench-c153g-ownership-grpc` (`-count=3`, 200 ms samples on Linux/amd64,
AMD Ryzen 9 5950X):

| Path | ns/op | B/op | allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| direct bounded collector callback | 8,259 | 3,081 | 41 | 1.00x |
| opt-in authenticated gRPC vote fetch | 50,023 | 17,878 | 255 | 6.06x |

Raw output:

```text
BenchmarkC153gPartitionOwnershipConsensusDirect-32        31916  7784 ns/op  3083 B/op   41 allocs/op
BenchmarkC153gPartitionOwnershipConsensusDirect-32        29391  8528 ns/op  3081 B/op   41 allocs/op
BenchmarkC153gPartitionOwnershipConsensusDirect-32        29060  8259 ns/op  3081 B/op   41 allocs/op
BenchmarkC153gPartitionOwnershipConsensusGRPCFetch-32     4579 49805 ns/op 17795 B/op  255 allocs/op
BenchmarkC153gPartitionOwnershipConsensusGRPCFetch-32     4628 50407 ns/op 17903 B/op  255 allocs/op
BenchmarkC153gPartitionOwnershipConsensusGRPCFetch-32     4296 50023 ns/op 17878 B/op  255 allocs/op
```

The gRPC path is intentionally more expensive because it adds protobuf
framing, metadata authentication, HMAC verification, server validation, and
the remote call boundary. It is disabled unless a vote handler is configured;
the local collector and existing cache paths are unchanged.
EOF
    apply_diff_from_base "$base" "$desired" BENCHMARK.md
  fi
  fi

  base="$tmp_dir/makefile.base"
  desired="$tmp_dir/makefile.desired"
  if index_contains "Makefile" '^stage-c153g-ownership-grpc:'; then
    return
  fi
  git show HEAD:Makefile > "$base"
  {
    cat <<'EOF'
.PHONY: test-c153g-ownership-grpc
test-c153g-ownership-grpc:
	@bash scripts/test-c153g-ownership-grpc.sh

.PHONY: format-c153g-ownership-grpc
format-c153g-ownership-grpc:
	@bash scripts/format-c153g-ownership-grpc.sh

.PHONY: bench-c153g-ownership-grpc
bench-c153g-ownership-grpc:
	@bash scripts/bench-c153g-ownership-grpc.sh

.PHONY: test-c153g-all
test-c153g-all:
	@bash scripts/test-c153g-all.sh

.PHONY: race-c153g-ownership-grpc
race-c153g-ownership-grpc:
	@bash scripts/race-c153g-ownership-grpc.sh

.PHONY: verify-c153g-ownership-grpc
verify-c153g-ownership-grpc:
	@bash scripts/verify-c153g-ownership-grpc.sh

.PHONY: test-c153g-packages
test-c153g-packages:
	@bash scripts/test-c153g-packages.sh

.PHONY: stage-c153g-ownership-grpc
stage-c153g-ownership-grpc:
	@bash scripts/deliver-c153g-ownership-grpc.sh stage

.PHONY: commit-c153g-ownership-grpc
commit-c153g-ownership-grpc:
	@bash scripts/deliver-c153g-ownership-grpc.sh commit

.PHONY: push-c153g-ownership-grpc
push-c153g-ownership-grpc:
	@bash scripts/deliver-c153g-ownership-grpc.sh push

.PHONY: deliver-c153g-ownership-grpc
deliver-c153g-ownership-grpc:
	@bash scripts/deliver-c153g-ownership-grpc.sh commit

EOF
    cat "$base"
  } > "$desired"
  apply_diff_from_base "$base" "$desired" Makefile
}

stage() {
  assert_no_unrelated_staged
  git add -- "${feature_files[@]}"
  stage_shared_files
  git diff --cached --check -- \
    BENCHMARK.md C153_PARTITION_OWNERSHIP_GRPC.md INSPIRATION.md Makefile \
    proto internal/gen/hatriecache/v1/cache.pb.go internal/gen/hatriecache/v1/cache_grpc.pb.go \
    hat/hatCache/grpc.go hat/hatCache/c153g_partition_ownership_consensus_grpc.go \
    hat/hatCache/c153g_partition_ownership_consensus_grpc_test.go \
    hat/hatCache/c153g_partition_ownership_consensus_grpc_benchmark_test.go scripts
  printf '%s\n' 'C153g feature paths staged.'
}

case "$action" in
  status)
    git status --short -- BENCHMARK.md C153_PARTITION_OWNERSHIP_GRPC.md INSPIRATION.md Makefile proto internal/gen/hatriecache/v1/cache.pb.go internal/gen/hatriecache/v1/cache_grpc.pb.go hat/hatCache/grpc.go hat/hatCache/c153g_partition_ownership_consensus_grpc.go hat/hatCache/c153g_partition_ownership_consensus_grpc_test.go hat/hatCache/c153g_partition_ownership_consensus_grpc_benchmark_test.go scripts/*c153g*
    ;;
  stage)
    stage
    ;;
  commit)
    stage
    assert_no_unrelated_staged
    git commit -m 'feat(topology): add authenticated ownership vote transport [skip ci]'
    ;;
  push)
    git push origin HEAD
    ;;
  *)
    printf 'usage: %s status|stage|commit|push\n' "$0" >&2
    exit 2
    ;;
esac
