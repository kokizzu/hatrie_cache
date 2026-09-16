#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(git diff --cached --name-only)" ]]; then
	printf '%s\n' 'Refusing to stage CH-18 while unrelated paths are already staged.' >&2
	git diff --cached --name-only >&2
	exit 1
fi

paths=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CH018_PROJECTION_REFRESH_STATUS.md
	INSPIRATION_BACKLOG.md
	README.md
	Makefile
	hat/hatSql/ch018_projection_refresh_status.go
	hat/hatSql/ch018_projection_refresh_status_test.go
	hat/hatSql/ch018_projection_refresh_status_benchmark_test.go
	hat/hatSql/incremental_projection.go
	scripts/benchmark-ch018-projection-refresh-status.sh
	scripts/format-ch018-projection-refresh-status-permanent.sh
	scripts/test-ch018-projection-refresh-status.sh
	scripts/test-race-ch018-projection-refresh-status.sh
	scripts/verify-ch018-projection-refresh-status.sh
	scripts/vet-ch018-projection-refresh-status.sh
)

git diff --check -- "${paths[@]}"

makefile_tmp=$(mktemp /tmp/hatrie-cache-ch018-makefile.XXXXXX)
adopted_tmp=$(mktemp /tmp/hatrie-cache-ch018-adopted.XXXXXX)
cleanup() {
	rm -f "$makefile_tmp" "$adopted_tmp"
}
trap cleanup EXIT

git show HEAD:Makefile > "$makefile_tmp"
printf '%s\n' \
	'.PHONY: format-ch018-projection-refresh-status' \
	'format-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/format-ch018-projection-refresh-status-permanent.sh' \
	'.PHONY: test-ch018-projection-refresh-status' \
	'test-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/test-ch018-projection-refresh-status.sh' \
	'.PHONY: test-race-ch018-projection-refresh-status' \
	'test-race-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/test-race-ch018-projection-refresh-status.sh' \
	'.PHONY: vet-ch018-projection-refresh-status' \
	'vet-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/vet-ch018-projection-refresh-status.sh' \
	'.PHONY: verify-ch018-projection-refresh-status' \
	'verify-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/verify-ch018-projection-refresh-status.sh' \
	'.PHONY: benchmark-ch018-projection-refresh-status' \
	'benchmark-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/benchmark-ch018-projection-refresh-status.sh' \
	'.PHONY: review-ch018-projection-refresh-status' \
	'review-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/review-ch018-projection-refresh-status.sh' \
	'.PHONY: stage-ch018-projection-refresh-status' \
	'stage-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/stage-ch018-projection-refresh-status.sh' \
	'.PHONY: commit-ch018-projection-refresh-status' \
	'commit-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/commit-ch018-projection-refresh-status.sh' \
	'.PHONY: push-ch018-projection-refresh-status' \
	'push-ch018-projection-refresh-status:' \
	$'\tbash ./scripts/push-ch018-projection-refresh-status.sh' >> "$makefile_tmp"
makefile_blob=$(git hash-object -w "$makefile_tmp")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:ADOPTED_QUERY_ENGINE_IDEAS.md > "$adopted_tmp"
awk -v row='| ClickHouse | Projection refresh lag and failure state | Adopted as a bounded incremental-projection status snapshot | `IncrementalProjectionRunner.Status` reports applied/observed sequence frontiers, lag, refresh state, bounded last error, timestamps, and consecutive failures without changing durable checkpoint semantics; the runner remains disabled by default. See [CH018_PROJECTION_REFRESH_STATUS.md](CH018_PROJECTION_REFRESH_STATUS.md) and [BENCHMARK.md#ch-018-projection-refresh-lag-and-failure-state](BENCHMARK.md#ch-018-projection-refresh-lag-and-failure-state). |' \
	'index($0, "| ClickHouse | Mutation dependency graph with resumable progress |") == 1 { print; print row; next } { print }' \
	"$adopted_tmp" > "$adopted_tmp.new"
mv "$adopted_tmp.new" "$adopted_tmp"
adopted_blob=$(git hash-object -w "$adopted_tmp")
git update-index --add --cacheinfo "100644,$adopted_blob,ADOPTED_QUERY_ENGINE_IDEAS.md"

git add -- \
	BENCHMARK.md \
	CH018_PROJECTION_REFRESH_STATUS.md \
	INSPIRATION_BACKLOG.md \
	README.md \
	hat/hatSql/ch018_projection_refresh_status.go \
	hat/hatSql/ch018_projection_refresh_status_test.go \
	hat/hatSql/ch018_projection_refresh_status_benchmark_test.go \
	hat/hatSql/incremental_projection.go \
	scripts/benchmark-ch018-projection-refresh-status.sh \
	scripts/format-ch018-projection-refresh-status-permanent.sh \
	scripts/test-ch018-projection-refresh-status.sh \
	scripts/test-race-ch018-projection-refresh-status.sh \
	scripts/verify-ch018-projection-refresh-status.sh \
	scripts/vet-ch018-projection-refresh-status.sh \
	scripts/review-ch018-projection-refresh-status.sh \
	scripts/stage-ch018-projection-refresh-status.sh \
	scripts/commit-ch018-projection-refresh-status.sh \
	scripts/push-ch018-projection-refresh-status.sh
git diff --cached --check -- "${paths[@]}"
git diff --cached --name-only -- "${paths[@]}"
