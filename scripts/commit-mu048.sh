#!/usr/bin/env bash
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
head=$(git rev-parse HEAD)
stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mu048-stage.XXXXXX")
archive=$(mktemp "${TMPDIR:-/tmp}/hatrie-mu048-archive.XXXXXX.tar")
index=$(mktemp "${TMPDIR:-/tmp}/hatrie-mu048-index.XXXXXX")
trap 'rm -rf "$stage" "$archive" "$index"' EXIT

git archive --format=tar --output="$archive" "$head"
tar -xf "$archive" -C "$stage"

copy_file() {
	local path=$1
	mkdir -p "$stage/$(dirname "$path")"
	cp "$repo/$path" "$stage/$path"
}

copy_file MU048_CONNECTOR_HEALTH_REMEDIATION.md
copy_file hat/hatPipeline/mu048_connector_health.go
copy_file hat/hatPipeline/mu048_connector_health_test.go
copy_file hat/hatPipeline/mu048_connector_health_benchmark_test.go
copy_file scripts/format-mu048.sh
copy_file scripts/test-mu048.sh
copy_file scripts/benchmark-mu048.sh
copy_file scripts/test-mu048-package.sh
copy_file scripts/race-mu048.sh
copy_file scripts/vet-mu048.sh
copy_file scripts/verify-mu048.sh
copy_file scripts/commit-mu048.sh
copy_file scripts/push-mu048.sh

cat >> "$stage/Makefile" <<'EOF'

# BEGIN MU048 connector health remediation
.PHONY: format-mu048 test-mu048 benchmark-mu048 test-mu048-package race-mu048 vet-mu048 verify-mu048 commit-mu048 push-mu048
format-mu048:
	bash ./scripts/format-mu048.sh
test-mu048:
	bash ./scripts/test-mu048.sh
benchmark-mu048:
	bash ./scripts/benchmark-mu048.sh
test-mu048-package:
	bash ./scripts/test-mu048-package.sh
race-mu048:
	bash ./scripts/race-mu048.sh
vet-mu048:
	bash ./scripts/vet-mu048.sh
verify-mu048:
	bash ./scripts/verify-mu048.sh
commit-mu048:
	bash ./scripts/commit-mu048.sh
push-mu048:
	bash ./scripts/push-mu048.sh
# END MU048 connector health remediation
EOF

cat >> "$stage/README.md" <<'EOF'

## Connector health remediation

Connector startup can use an opt-in bounded retry and quarantine policy without changing the existing lifecycle path. See [MU048_CONNECTOR_HEALTH_REMEDIATION.md](MU048_CONNECTOR_HEALTH_REMEDIATION.md) for the API, defaults, failure behavior, and benchmark.
EOF

cat >> "$stage/BENCHMARK.md" <<'EOF'

## MU-048 connector health remediation

The opt-in `StartWithHealthPolicy` path keeps the successful single-attempt case at `544 B/op` and `5 allocs/op` in the measured benchmark. Median timings were `435.5 ns/op` for the clean baseline, `426.8 ns/op` for existing `Start`, and `415.4 ns/op` for the policy path; the overlapping samples are treated as noise, not as a throughput claim. Failed attempts intentionally add bounded timer/backoff work. Raw samples and the test commands are documented in [MU048_CONNECTOR_HEALTH_REMEDIATION.md](MU048_CONNECTOR_HEALTH_REMEDIATION.md).
EOF

perl -0pi -e '
my $old = "| M-U48 | Source connector health remediation | Health status is observable, but no policy can pause, retry, or quarantine a failed connector with bounded backoff. | Avoid retry storms, preserve offsets, and operator override. |";
my $new = "| M-U48 | Source connector health remediation | Implemented: opt-in bounded retry, exponential backoff, context cancellation, and quarantine through `StartWithHealthPolicy`. | Avoid retry storms, preserve offsets, and operator override without changing the default lifecycle path. |";
my $count = s/\Q$old\E/$new/;
die "expected exactly one M-U48 matrix row, got $count\n" if $count != 1;
' "$stage/PRODUCT_IDEA_GAPS.md"

rm -f "$index"
export GIT_INDEX_FILE="$index"
git read-tree "$head"
git --work-tree="$stage" add -- \
	BENCHMARK.md \
	Makefile \
	MU048_CONNECTOR_HEALTH_REMEDIATION.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	hat/hatPipeline/mu048_connector_health.go \
	hat/hatPipeline/mu048_connector_health_test.go \
	hat/hatPipeline/mu048_connector_health_benchmark_test.go \
	scripts/benchmark-mu048.sh \
	scripts/commit-mu048.sh \
	scripts/format-mu048.sh \
	scripts/push-mu048.sh \
	scripts/race-mu048.sh \
	scripts/test-mu048-package.sh \
	scripts/test-mu048.sh \
	scripts/verify-mu048.sh \
	scripts/vet-mu048.sh
tree=$(git write-tree)
commit=$(git commit-tree "$tree" -p "$head" -m "feat: add opt-in connector health remediation")
git update-ref HEAD "$commit" "$head"

printf 'MU-048 isolated commit: %s\n' "$commit"
git diff-tree --stat --oneline "$commit"
