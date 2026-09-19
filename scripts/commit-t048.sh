#!/usr/bin/env bash
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
current_head=$(git rev-parse HEAD)
git fetch origin master
base=$(git rev-parse origin/master)
stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t048-stage.XXXXXX")
archive=$(mktemp "${TMPDIR:-/tmp}/hatrie-t048-archive.XXXXXX.tar")
index=$(mktemp "${TMPDIR:-/tmp}/hatrie-t048-index.XXXXXX")
trap 'rm -rf "$stage" "$archive" "$index"' EXIT

git archive --format=tar --output="$archive" "$base"
tar -xf "$archive" -C "$stage"

copy_file() {
	local path=$1
	mkdir -p "$stage/$(dirname "$path")"
	cp "$repo/$path" "$stage/$path"
}

copy_file T048_IDEMPOTENT_REMOTE_RETRY.md
copy_file hat/hatReplication/t048_remote_call.go
copy_file hat/hatReplication/t048_remote_call_test.go
copy_file hat/hatReplication/t048_remote_call_public_test.go
copy_file hat/hatReplication/t048_remote_call_benchmark_test.go
copy_file hat/hatReplication/t048_remote_call_baseline_benchmark_test.go
copy_file scripts/format-t048.sh
copy_file scripts/test-t048.sh
copy_file scripts/benchmark-t048.sh
copy_file scripts/test-t048-package.sh
copy_file scripts/race-t048.sh
copy_file scripts/vet-t048.sh
copy_file scripts/verify-t048.sh
copy_file scripts/commit-t048.sh
copy_file scripts/push-t048.sh

cat >> "$stage/Makefile" <<'EOF'

# BEGIN T048 idempotent remote-call retry policy
.PHONY: format-t048 test-t048 benchmark-t048 test-t048-package race-t048 vet-t048 verify-t048 commit-t048 push-t048
format-t048:
	bash ./scripts/format-t048.sh
test-t048:
	bash ./scripts/test-t048.sh
benchmark-t048:
	bash ./scripts/benchmark-t048.sh
test-t048-package:
	bash ./scripts/test-t048-package.sh
race-t048:
	bash ./scripts/race-t048.sh
vet-t048:
	bash ./scripts/vet-t048.sh
verify-t048:
	bash ./scripts/verify-t048.sh
commit-t048:
	bash ./scripts/commit-t048.sh
push-t048:
	bash ./scripts/push-t048.sh
# END T048 idempotent remote-call retry policy
EOF

cat >> "$stage/README.md" <<'EOF'

## Idempotent remote-call retries

Peer calls can use an opt-in method-aware retry policy that requires idempotency keys and fencing tokens for retryable writes and rejects retries for non-idempotent methods. See [T048_IDEMPOTENT_REMOTE_RETRY.md](T048_IDEMPOTENT_REMOTE_RETRY.md) for the API and measured overhead.
EOF

cat >> "$stage/BENCHMARK.md" <<'EOF'

## T-U48 idempotent remote-call retry policy

The successful one-attempt policy path measured 8.305 ns/op, 0 B/op, and 0 allocs/op versus a 0.524 ns/op inlined local callback control. The roughly 7.8 ns local policy cost is intentional opt-in validation/envelope work; network latency dominates it. Retry timers/backoff are only paid after a failed attempt. Raw samples and safety semantics are documented in [T048_IDEMPOTENT_REMOTE_RETRY.md](T048_IDEMPOTENT_REMOTE_RETRY.md).
EOF

perl -0pi -e '
my $old = "| T-U48 | Idempotent remote-call retry policy | Peer calls lack a method-aware retry/backoff policy tied to idempotency keys and fencing tokens. | No duplicate mutation, jitter, and observability. |";
my $new = "| T-U48 | Idempotent remote-call retry policy | Implemented: method-aware bounded retries, stable idempotency/fencing envelopes, optional jitter, and retry events; non-idempotent methods are single-attempt. | No duplicate mutation, jitter, and observability. |";
my $count = s/Q$oldE/$new/;
die "expected exactly one T-U48 matrix row, got $count" if $count != 1;
' "$stage/PRODUCT_IDEA_GAPS.md"

rm -f "$index"
export GIT_INDEX_FILE="$index"
git read-tree "$base"
git --work-tree="$stage" add -- BENCHMARK.md Makefile PRODUCT_IDEA_GAPS.md README.md T048_IDEMPOTENT_REMOTE_RETRY.md hat/hatReplication/t048_remote_call.go hat/hatReplication/t048_remote_call_benchmark_test.go hat/hatReplication/t048_remote_call_baseline_benchmark_test.go hat/hatReplication/t048_remote_call_public_test.go hat/hatReplication/t048_remote_call_test.go scripts/benchmark-t048.sh scripts/commit-t048.sh scripts/format-t048.sh scripts/push-t048.sh scripts/race-t048.sh scripts/test-t048-package.sh scripts/test-t048.sh scripts/verify-t048.sh scripts/vet-t048.sh
tree=$(git write-tree)
commit=$(git commit-tree "$tree" -p "$base" -m "feat: add idempotent remote-call retry policy")
git update-ref HEAD "$commit" "$current_head"
printf '%s' "T-U48 isolated commit: $commit"
printf '\n'
git diff-tree --stat --oneline "$commit"
